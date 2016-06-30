(ns onyx.plugin.rethinkdb
  (:require [clojure.core.async :as async]
            [onyx.peer
             [function :as function]
             [pipeline-extensions :as pipeline]]
            [onyx.static.default-vals :as defaults]
            [onyx.types :as types]
            [rethinkdb.query :as r]
            [taoensso.timbre :as timbre])
  (:import [java.util UUID]
           [rethinkdb.net Cursor]))

;;; Reader

(defn- start-read-loop! [log-prefix conn query read-ch]
  (async/thread
    (let [results-ch (r/run query conn {:async? true})]
      (loop [result (async/<!! results-ch)]
        (timbre/trace log-prefix "New query result" result)
        (cond
          (nil? result) (do (timbre/debug log-prefix "Query result channel closed, stopping read loop")
                            (async/>!! read-ch (types/input (UUID/randomUUID) :done)))
          (instance? Throwable result) (do (timbre/debug log-prefix "Query result channel returned a throwable" result)
                                           (async/>!! read-ch
                                                      (types/input (UUID/randomUUID)
                                                                   (ex-info "Uncaught exception in rethinkdb input query" {:exception result}))))
          (or (sequential? result)
              (instance? Cursor result)) (if-let [v (first result)]
                                           (when (async/>!! read-ch (types/input (UUID/randomUUID) v))
                                             (recur (rest result)))
                                           (async/>!! read-ch (types/input (UUID/randomUUID) :done)))
          :else (when (async/>!! read-ch (types/input (UUID/randomUUID) result))
                  (recur (async/<!! results-ch))))))))

(defn- done? [message]
  (= :done (:message message)))

(defn- all-done? [messages]
  (every? done? messages))

(defrecord RethinkDbReader [read-ch retry-ch pending-messages drained?
                            max-pending batch-size batch-timeout]
  pipeline/Pipeline
  (write-batch
    [_ event]
    (function/write-batch event))

  (read-batch
    [_ {:keys [onyx.core/log-prefix]}]
    (let [pending      (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch   (async/timeout batch-timeout)
          batch        (into [] (comp (keep (fn [_] (first (async/alts!! [retry-ch read-ch timeout-ch] :priority true))))
                                      (map (fn [msg] (if (done? msg)
                                                       (types/input (UUID/randomUUID) :done)
                                                       msg))))
                             (range max-segments))]
      (let [new-pending (vals (swap! pending-messages into (map (juxt :id identity)) batch))]
        (when (and (all-done? new-pending)
                   (all-done? batch)
                   (or (not (empty? new-pending))
                       (not (empty? batch))))
          (timbre/debug log-prefix "Input is drained")
          (reset! drained? true)))
      (when (pos? (count batch))
        (timbre/debugf "%s Producing batch of %s new segments" log-prefix (count batch)))
      {:onyx.core/batch batch}))

  pipeline/PipelineInput
  (ack-segment [_ _ message-id]
    (swap! pending-messages dissoc message-id))

  (retry-segment
    [_ _ message-id]
    (when-let [msg (get @pending-messages message-id)]
      (async/>!! retry-ch (types/input (UUID/randomUUID) (:message msg)))
      (swap! pending-messages dissoc message-id)))

  (pending?
    [_ _ message-id]
    (get @pending-messages message-id))

  (drained?
    [_ _]
    @drained?))

(defn inject-reader
  [{:keys [onyx.core/task-map onyx.core/pipeline onyx.core/log-prefix]} _]
  {:pre [(= 1 (:onyx/max-peers task-map))
         (:rethinkdb/query task-map)]}
  (let [host       (:rethinkdb/host task-map "localhost")
        port       (:rethinkdb/port task-map 28015)
        query      (:rethinkdb/query task-map)
        read-ch    (:read-ch pipeline)
        retry-ch   (:retry-ch pipeline)
        connection (r/connect :host host :port port)]
    (start-read-loop! log-prefix connection query read-ch)
    (timbre/debugf "%s Injecting rethinkdb reader for %s:%s" log-prefix host port)
    {:rethinkdb/connection connection
     :rethinkdb/read-ch    read-ch
     :rethinkdb/retry-ch   retry-ch}))

(defn stop-reader [{:keys [rethinkdb/connection rethinkdb/read-ch rethinkdb/retry-ch onyx.core/log-prefix]} _]
  (timbre/debug log-prefix "Closing read channel")
  (async/close! read-ch)
  (timbre/debug log-prefix "Closing retry channel")
  (async/close! retry-ch)
  (timbre/debug log-prefix "Closing rethinkdb connection")
  (.close connection)
  (timbre/debug log-prefix "Closed rethinkdb connection")
  (while (async/poll! read-ch))
  (timbre/debug log-prefix "Closed read channel")
  (while (async/poll! retry-ch))
  (timbre/debug log-prefix "Closed retry channel"))

(defn input [{:keys [onyx.core/task-map]}]
  (let [max-pending      (defaults/arg-or-default :onyx/max-pending task-map)
        batch-size       (defaults/arg-or-default :onyx/batch-size task-map)
        batch-timeout    (defaults/arg-or-default :onyx/batch-timeout task-map)
        read-buffer      (:rethinkdb/read-buffer task-map 1000)
        pending-messages (atom {})
        drained?         (atom false)
        read-ch          (async/chan read-buffer)
        retry-ch         (async/chan (* 2 max-pending))]
    (->RethinkDbReader
      read-ch retry-ch pending-messages drained? max-pending batch-size batch-timeout)))

(def reader-calls
  {:lifecycle/before-task-start inject-reader
   :lifecycle/after-task-stop   stop-reader})

;;; Writer

(defrecord RethinkDbWriter []
  pipeline/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results rethinkdb/connection onyx.core/log-prefix]}]
    (transduce
      (comp (mapcat :leaves)
            (map :message))
      (completing
        (fn [_ q]
          (let [res (r/run q connection)]
            (when (and (:inserted res)
                       (pos? (:errors res)))
              (timbre/warnf "%s Write query returned %s errors, first error was %s"
                            log-prefix (:errors res) (:first_error res)))
            (when (instance? Throwable res)
              (throw (ex-info "Uncaught exception in rethinkdb output query" {:exception res}))))))
      nil
      (:tree results))
    {})

  (seal-resource
    [_ _]
    {}))

(defn output [_]
  (->RethinkDbWriter))

(defn inject-writer [{:keys [onyx.core/task-map onyx.core/log-prefix]} _]
  (let [host (:rethinkdb/host task-map "localhost")
        port (:rethinkdb/port task-map 28015)]
    (timbre/debugf "%s Injecting rethinkdb writer for %s:%s" log-prefix host port)
    {:rethinkdb/connection (r/connect :host host :port port)}))

(defn stop-writer [{:keys [rethinkdb/connection onyx.core/log-prefix]} _]
  (timbre/debug log-prefix "Closing rethinkdb connection")
  (.close connection)
  (timbre/debug log-prefix "Closed rethinkdb connection"))



(def writer-calls
  {:lifecycle/before-task-start inject-writer
   :lifecycle/after-task-stop   stop-writer})
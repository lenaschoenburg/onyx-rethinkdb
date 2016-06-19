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
           [rethinkdb.net Cursor]
           [onyx.types Leaf]))

;;; Reader

(defn- start-read-loop! [log-prefix host port query read-ch]
  (async/thread
    (with-open [conn (r/connect :host host :port port)]
      (let [results-ch (r/run query conn {:async? true})]
        (loop [result (async/<!! results-ch)]
          (timbre/trace log-prefix "New query result" result)
          (cond
            (nil? result) (do (timbre/debug log-prefix "Query result channel closed, stopping read loop")
                              (async/close! read-ch))
            (instance? Throwable result) (do (timbre/debug log-prefix "Query result channel returned a throwable" result)
                                             (async/>!! read-ch (ex-info "uncaught exception in rethinkdb input query" {:exception result})))
            (or (sequential? result)
                (instance? Cursor result)) (if-let [v (first result)]
                                             (when (async/>!! read-ch v)
                                               (recur (rest result)))
                                             (async/close! read-ch))
            :else (when (async/>!! read-ch result)
                    (recur (async/<!! results-ch)))))))))

(def transform-query-result
  (fn [rf]
    (fn
      ([]
       (rf))
      ([res]
       (rf (rf res (types/input (UUID/randomUUID) :done))))
      ([res in]
       (rf res
           (if (instance? Leaf in)
             (assoc in :id (UUID/randomUUID))
             (types/input (UUID/randomUUID) in)))))))

(defn- done? [message]
  (= :done (:message message)))

(defn- all-done? [messages]
  (empty? (remove done? messages)))

(defrecord RethinkDbReader [read-ch pending-messages drained?
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
          batch        (->> (range max-segments)
                            (keep (fn [_] (first (async/alts!! [read-ch timeout-ch] :priority true)))))]
      (swap! pending-messages into (map (juxt :id identity)) batch)
      (let [new-pending (vals @pending-messages)]
        (when (and (all-done? new-pending)
                   (all-done? batch)
                   (or (not (empty? new-pending))
                       (not (empty? batch))))
          (timbre/debug log-prefix "Input is drained")
          (reset! drained? true)))
      (timbre/tracef log-prefix "Producing batch of %s new messages" (count batch))
      {:onyx.core/batch batch}))

  pipeline/PipelineInput
  (ack-segment [_ _ message-id]
    (swap! pending-messages dissoc message-id))

  (retry-segment
    [_ {:keys [onyx.core/log-prefix]} message-id]
    (when-let [msg (get @pending-messages message-id)]
      (timbre/trace log-prefix "Retrying message with id" message-id)
      (swap! pending-messages dissoc message-id)
      (async/>!! read-ch msg)))

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
  (let [host    (:rethinkdb/host task-map "localhost")
        port    (:rethinkdb/port task-map 28015)
        query   (:rethinkdb/query task-map)
        read-ch (:read-ch pipeline)]
    (start-read-loop! log-prefix host port query read-ch)
    (timbre/debugf log-prefix "Injecting rethinkdb reader for %s:%s" host port)
    {:rethinkdb/read-ch read-ch}))

(defn input [{:keys [onyx.core/task-map]}]
  (let [max-pending      (defaults/arg-or-default :onyx/max-pending task-map)
        batch-size       (defaults/arg-or-default :onyx/batch-size task-map)
        batch-timeout    (defaults/arg-or-default :onyx/batch-timeout task-map)
        read-buffer      (:rethinkdb/read-buffer task-map 1000)
        pending-messages (atom {})
        drained?         (atom false)
        read-ch          (async/chan read-buffer transform-query-result)]
    (->RethinkDbReader
      read-ch pending-messages drained? max-pending batch-size batch-timeout)))

(def reader-calls
  {:lifecycle/before-task-start inject-reader})

;;; Writer

(defrecord RethinkDbWriter []
  pipeline/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results rethinkdb/connection]}]
    (transduce
      (comp (mapcat :leaves)
            (map :message))
      (completing
        (fn [_ q]
          (let [res (r/run q connection)]
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
    (timbre/debugf log-prefix "Injecting rethinkdb writer for %s:%s" host port)
    {:rethinkdb/connection (r/connect :host host :port port)}))

(def writer-calls
  {:lifecycle/before-task-start inject-writer})
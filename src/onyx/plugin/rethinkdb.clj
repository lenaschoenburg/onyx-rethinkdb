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
           [rethinkdb.core Connection]))

;;; Reader

(defn put-result! [log-prefix read-ch result]
  (if (instance? Throwable result)
    (do (timbre/warn log-prefix "Query result channel returned a throwable" result)
        (async/>!! read-ch
                   (types/input (UUID/randomUUID)
                                (ex-info "Uncaught exception in rethinkdb input query" {:exception result}))))
    (async/>!! read-ch (types/input (UUID/randomUUID) result))))

(defn- start-read-loop! [{:keys [rethinkdb/host rethinkdb/port rethinkdb/db rethinkdb/query rethinkdb/reset-interval]}
                         log-prefix read-ch stop-ch]
  (async/thread
    (loop []
      (let [conn      (r/connect :host host :port port :db db)
            result-ch (r/run query conn {:async? true})
            reset-ch  (if reset-interval
                        (async/timeout reset-interval)
                        (async/chan))
            chs       [stop-ch reset-ch result-ch]
            next-op   (loop [[result ch] (async/alts!! chs :priority true)]
                        (cond
                          (= ch stop-ch)
                          ::stop

                          (= ch reset-ch)
                          ::reset

                          (nil? result)
                          (if reset-interval
                            ::reset
                            (do (async/>!! read-ch (types/input (UUID/randomUUID) :done))
                                ::stop))

                          (sequential? result)
                          (if (every? (partial put-result! log-prefix read-ch) result)
                            (recur (async/alts!! chs :priority true))
                            (if reset-interval
                              ::reset ::stop))

                          :else
                          (if (put-result! log-prefix read-ch result)
                            (recur (async/alts!! chs :priority true))
                            (if reset-interval
                              ::reset ::stop))))]
        (timbre/debugf "%s Closing connection to %s:%s" log-prefix host port)
        (.close conn)
        (timbre/debugf "%s Closed connection to %s:%s" log-prefix host port)
        (when (= ::reset next-op)
          (timbre/debugf "%s Reconnecting to %s:%s" log-prefix host port)
          (recur))))))

(defn- done? [message]
  (= :done (:message message)))

(defn- all-done? [messages]
  (every? done? messages))

(defrecord RethinkDbReader [stop-ch read-ch retry-ch pending-messages drained?
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
  (let [host     (:rethinkdb/host task-map "localhost")
        port     (:rethinkdb/port task-map 28015)
        read-ch  (:read-ch pipeline)
        retry-ch (:retry-ch pipeline)
        stop-ch  (:stop-ch pipeline)]
    (start-read-loop! task-map log-prefix read-ch stop-ch)
    (timbre/debugf "%s Injecting rethinkdb reader for %s:%s" log-prefix host port)
    {:rethinkdb/read-ch  read-ch
     :rethinkdb/retry-ch retry-ch
     :rethinkdb/stop-ch  stop-ch}))

(defn stop-reader [{:keys [rethinkdb/read-ch rethinkdb/retry-ch rethinkdb/stop-ch onyx.core/log-prefix]} _]
  (timbre/debug log-prefix "Closing read channel")
  (async/close! read-ch)
  (timbre/debug log-prefix "Closing retry channel")
  (async/close! retry-ch)
  (while (async/poll! read-ch))
  (timbre/debug log-prefix "Closed read channel")
  (while (async/poll! retry-ch))
  (timbre/debug log-prefix "Closed retry channel")
  (timbre/debug log-prefix "Stopping read loop")
  (async/close! stop-ch)
  (timbre/debug log-prefix "Stopped read loop"))


(defn input [{:keys [onyx.core/task-map]}]
  (let [max-pending      (defaults/arg-or-default :onyx/max-pending task-map)
        batch-size       (defaults/arg-or-default :onyx/batch-size task-map)
        batch-timeout    (defaults/arg-or-default :onyx/batch-timeout task-map)
        read-buffer      (:rethinkdb/read-buffer task-map 1000)
        pending-messages (atom {})
        drained?         (atom false)
        read-ch          (async/chan read-buffer)
        retry-ch         (async/chan (* 2 max-pending))
        stop-ch          (async/chan)]
    (->RethinkDbReader
      stop-ch read-ch retry-ch pending-messages drained? max-pending batch-size batch-timeout)))

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
              (timbre/warnf "%s Query returned %s errors" log-prefix (:errors res)))
            (when (instance? Throwable res)
              (throw (ex-info "Uncaught exception in query" {:exception res}))))))
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
        port (:rethinkdb/port task-map 28015)
        db   (:rethinkdb/db   task-map)]
    (timbre/debugf "%s Injecting writer for %s:%s" log-prefix host port)
    {:rethinkdb/connection (r/connect :host host :port port :db db)}))

(defn stop-writer [{:keys [rethinkdb/connection onyx.core/log-prefix]} _]
  (timbre/debug log-prefix "Closing connection")
  (.close ^Connection connection)
  (timbre/debug log-prefix "Closed connection"))

(def writer-calls
  {:lifecycle/before-task-start inject-writer
   :lifecycle/after-task-stop   stop-writer})
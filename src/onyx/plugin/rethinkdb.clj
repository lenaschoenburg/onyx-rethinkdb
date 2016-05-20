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

(defn- start-read-loop! [host port query read-ch]
  (async/thread
    (with-open [conn (r/connect :host host :port port)]
      (let [results-ch (r/run query conn {:async? true})]
        (loop [result (async/<!! results-ch)]
          (cond
            (nil? result) (async/close! read-ch)
            (instance? Throwable result) (async/>!! read-ch (ex-info "Uncaught exception in rethinkdb input query" {:exception result}))
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
       (rf res (types/input (UUID/randomUUID) in))))))

(defn- done? [message]
  (= :done (:message message)))

(defn- all-done? [messages]
  (empty? (remove done? messages)))

(defrecord RethinkDbReader [task-id log read-ch
                            pending-messages drained? acked-result
                            max-pending batch-size batch-timeout]
  pipeline/Pipeline
  (write-batch
    [_ event]
    (function/write-batch event))

  (read-batch
    [_ _]
    (let [pending      (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch   (async/timeout batch-timeout)
          batch        (->> (range max-segments)
                            (keep (fn [_] (first (async/alts!! [read-ch timeout-ch] :priority true)))))]
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) m))
      (let [new-pending (vals @pending-messages)]
        (when (and (all-done? new-pending)
                   (all-done? batch)
                   (or (not (empty? new-pending))
                       (not (empty? batch))))
          (reset! drained? true)))
      {:onyx.core/batch batch}))

  pipeline/PipelineInput
  (ack-segment [_ _ message-id]
    (swap! pending-messages dissoc message-id))

  (retry-segment
    [_ _ message-id]
    (when-let [msg (get @pending-messages message-id)]
      (swap! pending-messages dissoc message-id)
      (async/>!! read-ch (assoc msg :id (UUID/randomUUID)))))

  (pending?
    [_ _ message-id]
    (get @pending-messages message-id))

  (drained?
    [_ _]
    @drained?))

(defn inject-reader
  [{:keys [onyx.core/task-map onyx.core/pipeline]} _]
  {:pre [(= 1 (:onyx/max-peers task-map))
         (:rethinkdb/query task-map)]}
  (let [host (:rethinkdb/host task-map "localhost")
        port (:rethinkdb/port task-map 28015)
        query (:rethinkdb/query task-map)
        read-ch (:read-ch pipeline)]
    (start-read-loop! host port query read-ch)
    (timbre/spy :debug "Injecting read channel"
      {:rethinkdb/read-ch read-ch})))

(defn input [{:keys [onyx.core/log
                     onyx.core/task-id
                     onyx.core/task-map]}]
  (let [max-pending (defaults/arg-or-default :onyx/max-pending task-map)
        batch-size (defaults/arg-or-default :onyx/batch-size task-map)
        batch-timeout (defaults/arg-or-default :onyx/batch-timeout task-map)
        read-buffer (:rethinkdb/read-buffer task-map 1000)
        pending-messages (atom {})
        drained? (atom false)
        acked-result (atom nil)
        read-ch (async/chan read-buffer transform-query-result)]
    (->RethinkDbReader
      task-id log read-ch
      pending-messages drained? acked-result
      max-pending batch-size batch-timeout)))

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

(defn inject-writer [{:keys [onyx.core/task-map]} _]
  (let [host (:rethinkdb/host task-map "localhost")
        port (:rethinkdb/port task-map 28015)]
    {:rethinkdb/connection (r/connect :host host :port port)}))

(def writer-calls
  {:lifecycle/before-task-start inject-writer})
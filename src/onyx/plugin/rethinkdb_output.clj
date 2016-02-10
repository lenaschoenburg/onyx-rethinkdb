(ns onyx.plugin.rethinkdb-output
  (:require [onyx.peer.function :as function]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.peer.pipeline-extensions :as pipeline]
            [rethinkdb.query :as r]))

(defrecord RethinkDbWriter [table get-in-keys]
  pipeline/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results rethinkdb/connection]}]
    (let [documents (into [] (comp (mapcat :leaves)
                                   (map :message)
                                   (map #(get-in % get-in-keys)))
                          (:tree results))]
      (-> table
          (r/insert documents)
          (r/run connection)))
    {})

  (seal-resource
    [_ _]
    {}))

(defn output [{:keys [onyx.core/task-map]}]
  (let [table (:rethinkdb/table task-map)
        get-in-keys (:rethinkdb/get-in task-map)]
    (->RethinkDbWriter table get-in-keys)))

(defn inject-writer [{:keys [onyx.core/task-map]} _]
  (let [host (:rethinkdb/host task-map "localhost")
        port (:rethinkdb/port task-map 28015)]
    {:rethinkdb/connection (r/connect :host host :port port)}))

(def writer-calls
  {:lifecycle/before-task-start inject-writer})
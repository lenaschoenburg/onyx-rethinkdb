(ns onyx.plugin.rethinkdb-test
  (:require [clojure.test :refer :all]
            [onyx.tasks.rethinkdb :as rethinkdb]
            [onyx.plugin.rethinkdb]
            [onyx.test-helper :as test-helper]
            [onyx.api :as onyx]
            [onyx.job :as job]
            [taoensso.timbre :as timbre]
            [rethinkdb.query :as r])
  (:import [java.util UUID]))

(def test-host "localhost")
(def test-port 28015)
(def test-db "onyx_rethinkdb_test_db")

(def test-documents
  [{:a 1}
   {:b 2}
   {:c 3}])

(defn load-in []
  (with-open [conn (r/connect :host test-host :port test-port)]
    (-> (r/db test-db)
        (r/table "test_in")
        (r/run conn)
        (vec))))

(defn load-out []
  (with-open [conn (r/connect :host test-host :port test-port)]
    (-> (r/db test-db)
        (r/table "test_out")
        (r/run conn)
        (vec))))

(defn bootstrap-db []
  (with-open [conn (r/connect :host test-host :port test-port)]
    (-> (r/db-create test-db)
        (r/run conn))
    (-> (r/db test-db)
        (r/table-create "test_in")
        (r/run conn))
    (-> (r/db test-db)
        (r/table-create "test_out")
        (r/run conn))
    (-> (r/db test-db)
        (r/table "test_in")
        (r/insert test-documents)
        (r/run conn))))

(defn drop-db []
  (with-open [conn (r/connect :host test-host :port test-port)]
    (-> (r/db-drop test-db)
        (r/run conn))))

(use-fixtures :each
  (fn [test-fn]
    (bootstrap-db)
    (test-fn)
    (drop-db)))


(defn env-config [id]
  {:onyx/tenancy-id       id
   :zookeeper/address     "localhost:2188"
   :zookeeper/server?     true
   :zookeeper.server/port 2188})

(defn peer-config [id]
  {:onyx/tenancy-id                       id
   :zookeeper/address                     "localhost:2188"
   :onyx.peer/job-scheduler               :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit?   true
   :onyx.messaging/impl                   :aeron
   :onyx.messaging/peer-port              40200
   :onyx.messaging/bind-addr              "localhost"})

(def base-job
  {:workflow        [[:load-documents :save-documents]]
   :task-scheduler  :onyx.task-scheduler/balanced
   :catalog         []
   :lifecycles      []
   :windows         []
   :triggers        []
   :flow-conditions []})

(defn submit-and-wait
  ([peer-config]
   (timbre/debug "Submitting job")
   (->> (-> base-job
            (job/add-task (rethinkdb/input
                            :load-documents
                            {:onyx/batch-size 10
                             :rethinkdb/query (-> (r/db test-db)
                                                  (r/table "test_in"))}))
            (job/add-task (rethinkdb/output
                            :save-documents
                            {:onyx/batch-size 10
                             :rethinkdb/table (-> (r/db test-db)
                                                  (r/table "test_out"))})))
        (onyx/submit-job peer-config)
        :job-id
        (onyx/await-job-completion peer-config))))

(deftest test-run
  (let [id (UUID/randomUUID)
        env-config (env-config id)
        peer-config (peer-config id)]
    (timbre/with-merged-config {:appenders {:println {:min-level :trace}}}
      (test-helper/with-test-env [test-env [3 env-config peer-config]]
        (submit-and-wait peer-config)
        (is (= (load-in) (load-out)))))))
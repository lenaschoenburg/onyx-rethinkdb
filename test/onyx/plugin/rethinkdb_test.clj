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

(timbre/handle-uncaught-jvm-exceptions!)

(def test-host "localhost")
(def test-port 28015)
(def test-db "onyx_rethinkdb_test_db")

(def test-documents
  (let [prog (volatile! 0)]
    (into []
          (repeatedly
            5000
            (fn []
              {:val (vswap! prog inc)})))))

(defn load-in []
  (with-open [conn (r/connect :host test-host :port test-port)]
    (-> (r/db test-db)
        (r/table "test_in" {"read-mode" "majority"})
        (r/count)
        (r/run conn))))

(defn load-out []
  (with-open [conn (r/connect :host test-host :port test-port)]
    (-> (r/db test-db)
        (r/table "test_out" {"read-mode" "majority"})
        (r/count)
        (r/run conn))))

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

(defn write-query [segment]
  (-> (r/db test-db)
      (r/table "test_out")
      (r/insert segment)))

(def job
  (-> base-job
      (job/add-task (rethinkdb/input
                      :load-documents
                      {:onyx/batch-size 1
                       :rethinkdb/query (-> (r/db test-db)
                                            (r/table "test_in" {"read-mode" "majority"}))}))
      (job/add-task (rethinkdb/output
                      :save-documents
                      {:onyx/batch-size 30
                       :onyx/fn ::write-query
                       :onyx/max-peers 1}))))

(defn submit-and-wait
  [peer-config]
  (->> job
       (onyx/submit-job peer-config)
       :job-id
       (onyx/await-job-completion peer-config)))

(deftest test-run
  (let [id (UUID/randomUUID)
        env-config (env-config id)
        peer-config (peer-config id)]
    (test-helper/with-test-env [test-env [8 env-config peer-config]]
      (submit-and-wait peer-config)
      (is (= (load-in) (load-out))))))
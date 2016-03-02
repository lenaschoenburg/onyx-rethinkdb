(ns onyx.plugin.rethinkdb-test
  (:require [clojure.test :refer :all]
            [onyx.plugin.rethinkdb-output]
            [onyx.plugin.rethinkdb-input]
            [onyx.test-helper :as test-helper]
            [onyx.api :as onyx]
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

(def catalog
  [{:onyx/name       :load-documents
    :onyx/plugin     :onyx.plugin.rethinkdb-input/input
    :onyx/medium     :rethinkdb
    :onyx/max-peers  1
    :onyx/type       :input
    :onyx/batch-size 100
    :onyx/bulk?      true
    :rethinkdb/host  test-host
    :rethinkdb/port  test-port
    :rethinkdb/query (-> (r/db test-db)
                         (r/table "test_in"))
    :onyx/doc        "Read documents from RethinkDB server"}

   {:onyx/name       :save-documents
    :onyx/plugin     :onyx.plugin.rethinkdb-output/output
    :onyx/type       :output
    :onyx/batch-size 100
    :onyx/bulk?      true
    :onyx/medium     :rethinkdb
    :onyx/max-peers  1
    :rethinkdb/host  test-host
    :rethinkdb/port  test-port
    :rethinkdb/table (-> (r/db test-db)
                         (r/table "test_out"))
    :onyx/doc        "Writes documents to a RethinkDB server"}])

(def workflow [[:load-documents :save-documents]])

(def lifecycles
  [{:lifecycle/task  :load-documents
    :lifecycle/calls :onyx.plugin.rethinkdb-input/reader-calls}
   {:lifecycle/task  :save-documents
    :lifecycle/calls :onyx.plugin.rethinkdb-output/writer-calls}])

(defn submit-and-wait
  ([peer-config catalog lc]
   (timbre/debug "Submitting job")
   (let [job-info (onyx/submit-job
                    peer-config
                    {:catalog        catalog
                     :workflow       workflow
                     :lifecycles     lc
                     :task-scheduler :onyx.task-scheduler/balanced})]
     (timbre/debug "Submitted Job " (:job-id job-info))
     (onyx/await-job-completion peer-config (:job-id job-info))
     (timbre/debug "Job completed")
     job-info)))

(deftest test-run
  (let [id (UUID/randomUUID)
        env-config (env-config id)
        peer-config (peer-config id)]
    (timbre/with-merged-config {:appenders {:println {:min-level :trace}}}
      (test-helper/with-test-env [test-env [3 env-config peer-config]]
        (submit-and-wait peer-config catalog lifecycles)
        (is (= (load-in)
               (load-out)))))))

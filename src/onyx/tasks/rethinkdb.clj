(ns onyx.tasks.rethinkdb
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(s/defschema Port
  (s/constrained s/Int #(<= 1 % 65535)))

(s/defschema RethinkDbTaskConfig
  {(s/optional-key :rethinkdb/host)            s/Str
   (s/optional-key :rethinkdb/port)            Port
   (s/optional-key :rethinkdb/db)              s/Str
   (s/optional-key :rethinkdb/token)           s/Int
   (s/optional-key :rethinkdb/auth-key)        s/Str
   (s/optional-key :rethinkdb/connect-timeout) s/Str
   (os/restricted-ns :rethinkdb)               s/Any})

(s/defschema RethinkDbInputTaskMap
  (assoc RethinkDbTaskConfig
    :rethinkdb/query {s/Any s/Any}))

(s/defschema RethinkDbOutputTaskMap
  RethinkDbTaskConfig)

(s/defschema RethinkDbInputTask
  {:task-map   RethinkDbInputTaskMap})

(s/defschema RethinkDbOutputTask
  {:task-map   RethinkDbOutputTaskMap})

(s/defn ^:always-validate input :- {:task s/Any :schema s/Any}
  [task-name :- s/Keyword opts :- {s/Any s/Any}]
  {:task   {:task-map   (merge opts
                               {:onyx/name      task-name
                                :onyx/type      :input
                                :onyx/medium    :rethinkdb
                                :onyx/plugin    :onyx.plugin.rethinkdb/input
                                :onyx/max-peers 1})
            :lifecycles [{:lifecycle/task  task-name
                          :lifecycle/calls :onyx.plugin.rethinkdb/reader-calls}]}
   :schema RethinkDbInputTask})

(s/defn ^:always-validate output :- {:task s/Any :schema s/Any}
  [task-name :- s/Keyword opts :- {s/Any s/Any}]
  {:task   {:task-map   (merge opts
                               {:onyx/name   task-name
                                :onyx/type   :output
                                :onyx/medium :rethinkdb
                                :onyx/plugin :onyx.plugin.rethinkdb/output})
            :lifecycles [{:lifecycle/task  task-name
                          :lifecycle/calls :onyx.plugin.rethinkdb/writer-calls}]}
   :schema RethinkDbOutputTask})
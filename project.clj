(defproject onyx-rethinkdb "0.8.9.0-SNAPSHOT"
  :description "Onyx plugin for rethinkdb"
  :url "FIX ME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.onyxplatform/onyx "0.8.9"]
                 [com.apa512/rethinkdb "0.11.0"]]
  :profiles {:dev {:dependencies []
                   :plugins []}})

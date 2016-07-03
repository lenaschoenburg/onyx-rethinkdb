# onyx-rethinkdb
Onyx Plugin providing input and output functions for rethinkdb

# Installation

In your project file:

```
[dignati/onyx-rethinkdb "0.9.6.2-SNAPSHOT"]
```

In your peer boot-up namespace:

```
(:require [onyx.plugin.rethinkdb])
```

# Functions

## Input

The input task can execute arbitrary rethinkdb queries and returns the results. Queries are generated and executed by
the [clj-rethinkdb] library. Require the namespace with

```
(:require [rethinkdb.query :as r])
```

### Catalog entry

```
{:onyx/name             :load-documents
 :onyx/type             :input
 :onyx/medium           :rethinkdb
 :onyx/plugin           :onyx.plugin.rethinkdb/input
 :onyx/max-peers        1
 :onyx/batch-size       100
 :onyx/max-pending      500
 :rethinkdb/query       (-> (r/db "test_db")
                            (r/table "test_in"))}
```

### Lifecycle entry

```
{:lifecycle/task  :load-documents
 :lifecycle/calls :onyx.plugin.rethinkdb/reader-calls}
```

### Task bundle

Instead of writing the catalog and lifecycle entries by hand you can also use a task bundle.
Require the helpers and rethinkdb:

```
(:require [onyx.job :as job]
          [onyx.tasks.rethinkdb :as rethinkdb]
          [rethinkdb.query :as r])
```

Then, add the input function to a base job:

```
(def job
  (-> base-job
      (job/add-task (rethinkdb/input
                      :load-documents
                      {:onyx/batch-size   100
                       :rethinkdb/query   (-> (r/db "test_db")
                                              (r/table "test_in"))}))))
```

### Attributes

| key                        | type      | default       | description
|----------------------------|-----------|---------------|-------------
|`:rethinkdb/host`           | `string`  | `"localhost"` | RethinkDB server host
|`:rethinkdb/port`           | `number`  | `28015`       | RethinkDB server port.
|`:rethinkdb/db`             | `string`  |               | Standard database for queries. The database can also be set inside the query.
|`:rethinkdb/query`          | `query`   |               | RethinkDB query to be executed. Constructed with the [clj-rethinkdb] library.
|`:rethinkdb/read-buffer`    | `number`  | `1000`        | Internal read buffer. Important in combination with `:rethinkdb/reset-interval`.
|`:rethinkdb/reset-interval` | `number`  |               | Time in milliseconds until the current query is stopped and restarted. Useful for changefeeds.


## Output

Similar to the input function, the output function executes arbitrary RethinkDB queries that are passed to it.

Require the namespace for the [clj-rethinkdb] library:

```
(:require [rethinkdb.query :as r])
```


### Catalog entry

```
{:onyx/name       :save-documents
 :onyx/type       :output
 :onyx/medium     :rethinkdb
 :onyx/plugin     :onyx.plugin.rethinkdb/output
 :onyx/batch-size 100}
```

### Lifecycle entry

```
{:lifecycle/task  :save-documents
 :lifecycle/calls :onyx.plugin.rethinkdb/save-calls}
```

### Task bundle

Again, you can save ypur time by using the task bundle:

Require the helpers and rethinkdb:

```
(:require [onyx.job :as job]
          [onyx.tasks.rethinkdb :as rethinkdb]
          [rethinkdb.query :as r])
```

Then, add the input function to a base job:

```
(def job
  (-> base-job
      (job/add-task (rethinkdb/output :save-documents {:onyx/batch-size 100}))))
```

### Attributes

| key                        | type      | default       | description
|----------------------------|-----------|---------------|-------------
|`:rethinkdb/host`           | `string`  | `"localhost"` | RethinkDB server host
|`:rethinkdb/port`           | `number`  | `28015`       | RethinkDB server port.
|`:rethinkdb/db`             | `string`  |               | Standard database for queries. The database can also be set inside the query.

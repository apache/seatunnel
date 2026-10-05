import ChangeLog from '../changelog/connector-cdc-mongodb.md';

# MongoDB CDC

> MongoDB CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>

## Key Features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The MongoDB CDC connector allows for reading snapshot data and incremental data from MongoDB database.

## Supported DataSource Info

In order to use the Mongodb CDC connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                                |
|------------|--------------------|-------------------------------------------------------------------------------------------|
| MongoDB    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cdc-mongodb) |

## Availability Settings

1. MongoDB version: MongoDB version >= 4.0.

2. Cluster deployment: replica sets or sharded clusters.

3. Storage Engine: WiredTiger Storage Engine.

4. Permissions: `changeStream` and `read`.

```javascript
// 1) Switch to the target database
use <DB_NAME>

// 2) Create role (common permissions for CDC scenarios)
db.createRole({
  role: "<ROLE_NAME>",
  privileges: [
    {
      resource: { db: "<DB_NAME>", collection: "" },
      actions: [
        "collStats",
        "splitVector",
        "listDatabases",
        "find",
        "listCollections",
        "changeStream"
      ]
    }
  ],
  roles: []
})

// 3) Create user and bind read + custom role
db.createUser({
  user: "<USER_NAME>",
  pwd: "<PASSWORD>",
  roles: [
    { role: "read", db: "<DB_NAME>" },
    { role: "<ROLE_NAME>", db: "<DB_NAME>" }
  ]
})

// 4) Grant additional role to user (use when user exists or additional authorization is needed)
db.grantRolesToUser("<USER_NAME>", ["<ROLE_NAME>"])
```

## Data Type Mapping

The following table lists the field data type mapping from MongoDB BSON type to Seatunnel data type.

| MongoDB BSON Type | SeaTunnel Data Type |
|-------------------|---------------------|
| ObjectId          | STRING              |
| String            | STRING              |
| Boolean           | BOOLEAN             |
| Binary            | BINARY              |
| Int32             | INTEGER             |
| Int64             | BIGINT              |
| Double            | DOUBLE              |
| Decimal128        | DECIMAL             |
| Date              | DATE                |
| Timestamp         | TIMESTAMP           |
| Object            | ROW                 |
| Array             | ARRAY               |

For specific types in MongoDB, we use Extended JSON format to map them to Seatunnel STRING type.

| MongoDB BSON type |                                       SeaTunnel STRING                                       |
|-------------------|----------------------------------------------------------------------------------------------|
| Symbol            | {"_value": {"$symbol": "12"}}                                                                |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}                       |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}}                                           |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**Tips**

> 1.When using the DECIMAL type in SeaTunnel, be aware that the maximum range cannot exceed 34 digits, which means you should use decimal(34, 18).<br/>

## Source Options

| Name                               | Type    | Required | Default | Description                                                                                                                                                                                                                                                               |
|------------------------------------|---------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hosts                              | String  | Yes      | -       | The comma-separated list of hostname and port pairs of the MongoDB servers, or a standard MongoDB connection URI using `mongodb://` or `mongodb+srv://`. For example: `localhost:27017,localhost:27018` or `mongodb+srv://cluster.example.net`.                         |
| username                           | String  | No       | -       | Name of the database user to be used when connecting to MongoDB. Required only when MongoDB authentication is enabled.                                                                                                                                                    |
| password                           | String  | No       | -       | Password to be used when connecting to MongoDB. Required only when MongoDB authentication is enabled.                                                                                                                                                                     |
| database                           | List    | Yes      | -       | Database names to watch for changes. Regular expressions are supported, for example `["inventory"]` or `["db.*"]`.                                                                                                                                                      |
| collection                         | List    | Yes      | -       | Collection names to watch for changes. Each value should use the fully qualified `database.collection` format, for example `["inventory.products", "inventory.orders"]`. Regular expressions are supported.                                                             |
| schema                             | Config  | No       | -       | Schema for one collection, including field names, field types, and optional primary key. For more details, see [Schema Feature](../../introduction/concepts/schema-feature.md).                                                                                          |
| tables_configs                     | List    | No       | -       | Schema list for multiple collections. Each item contains a `schema` block. The number and order of `tables_configs` items must match the `collection` list.                                                                                                               |
| connection.options                 | String  | No       | -       | Ampersand-separated MongoDB connection options. For example: `replicaSet=test&connectTimeoutMS=300000`.                                                                                                                                                                  |
| batch.size                         | Integer | No       | 1024    | Cursor batch size used when reading snapshot data.                                                                                                                                                                                                                       |
| poll.max.batch.size                | Integer | No       | 1024    | Maximum number of change stream documents to include in one poll batch.                                                                                                                                                                                                   |
| poll.await.time.ms                 | Integer | No       | 1000    | Time in milliseconds to wait before checking for new change stream results.                                                                                                                                                                                               |
| heartbeat.interval.ms              | Integer | No       | 0       | Time in milliseconds between heartbeat messages. Use `0` to disable heartbeat messages.                                                                                                                                                                                   |
| incremental.snapshot.chunk.size.mb | Integer | No       | 64      | Chunk size, in MB, for incremental snapshot reading.                                                                                                                                                                                                                     |
| startup.mode                       | Enum    | No       | INITIAL | Optional startup mode for MongoDB CDC consumer. Valid values are `initial`, `latest`, and `timestamp`. See the [Startup Mode](#startup-mode) section below.                                                                                                               |
| startup.timestamp                  | Long    | No       | -       | Start from the specified epoch timestamp in milliseconds. Only used when `startup.mode` is `timestamp`.                                                                                                                                                                   |
| stop.mode                          | Enum    | No       | NEVER   | Optional stop mode for MongoDB CDC consumer. Valid values are `never` and `timestamp`. See the [Stop Mode](#stop-mode) section below.                                                                                                                                      |
| stop.timestamp                     | Long    | No       | -       | Stop at the change-stream position derived from this epoch timestamp in milliseconds. Only used when `stop.mode` is `timestamp`.                                                                                                                                           |
| exactly_once                       | Boolean | No       | false   | Enable exactly-once semantics. Enabling this may increase memory usage during large table snapshot recovery.                                                                                                                                                              |
| debezium                           | Config  | No       | -       | Pass-through Debezium properties used by the embedded engine.                                                                                                                                                                                                             |
| common-options                     |         | No       | -       | Source plugin common parameters. For details, see [Source Common Options](../common-options/source-common-options.md).                                                                                                                                                    |

### Startup Mode

The `startup.mode` option controls where the connector starts reading when a job is submitted:

- `initial` (default): reads a snapshot of the monitored collections first, then switches to the change stream.
- `latest`: skips the snapshot entirely and starts from the latest change-stream position, so only changes made after the job starts are captured. Snapshot-related options such as `incremental.snapshot.chunk.size.mb` are ignored in this mode.
- `timestamp`: skips the snapshot and starts reading the change stream from the position given by `startup.timestamp`.

When a job is restored from a checkpoint or savepoint, it resumes from the checkpointed change-stream position regardless of `startup.mode`, so a restart never falls back to a new snapshot.

For example, to consume only changes made after the job starts:

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    startup.mode = "latest"
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

### Stop Mode

The `stop.mode` option controls whether the connector runs continuously or finishes at a bounded change-stream position:

- `never` (default): keeps reading the change stream.
- `timestamp`: reads until the MongoDB change-stream timestamp reaches the position derived from `stop.timestamp`, drains the records already produced by the source, and then finishes the job.

MongoDB change-stream timestamps have second precision. `stop.timestamp` is supplied as epoch milliseconds and converted to that timestamp representation. Events after the stop position are not emitted.

- A timestamp startup and timestamp stop must resolve to different positions, with the stop position later than the startup position. Values within the same second resolve to the same position and are rejected.
- Timestamp stop mode makes the source bounded, so a streaming job finishes after every split reaches the stop position.
- With `startup.mode = initial`, the initial snapshot is read completely. The stop timestamp only bounds the incremental change-stream phase.
- With `startup.mode = latest`, a stop position that has already passed produces no incremental records and the bounded source finishes.
- After checkpoint or savepoint restore, the stop position stored in the restored split takes precedence over a changed `stop.timestamp` in the submitted configuration.
- On an idle bounded stream, the connector checks MongoDB cluster time once per poll so it can finish even when no change event reaches the boundary.

For example, to read a bounded interval:

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    startup.mode = "timestamp"
    startup.timestamp = 1785542400000
    stop.mode = "timestamp"
    stop.timestamp = 1785546000000
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

### Tips

> 1.If the collection changes at a slow pace, it is strongly recommended to set an appropriate value greater than 0 for the heartbeat.interval.ms parameter. When we recover a Seatunnel job from a checkpoint or savepoint, the heartbeat events can push the resumeToken forward to avoid its expiration.<br/>
> 2.MongoDB has a limit of 16MB for a single document. Change documents include additional information, so even if the original document is not larger than 15MB, the change document may exceed the 16MB limit, resulting in the termination of the Change Stream operation.<br/>
> 3.It is recommended to use immutable shard keys. In MongoDB, shard keys allow modifications after transactions are enabled, but changing the shard key can cause frequent shard migrations, resulting in additional performance overhead. Additionally, modifying the shard key can also cause the Update Lookup feature to become ineffective, leading to inconsistent results in CDC (Change Data Capture) scenarios.<br/>
> 4. `schema` and `tables_configs` are mutually exclusive. Use `schema` for one collection and `tables_configs` for multiple collections.

## Change Streams

[**Change Stream**](https://www.mongodb.com/docs/v5.0/changeStreams/) is a new feature provided by MongoDB 3.6 for replica sets and sharded clusters that allows applications to access real-time data changes without the complexity and risk of tailing the oplog.
Applications can use change streams to subscribe to all data changes on a single collection, a database, or an entire deployment, and immediately react to them.

**Lookup Full Document for Update Operations** is a feature provided by **Change Stream** which can configure the change stream to return the most current majority-committed version of the updated document. Because of this feature, we can easily collect the latest full document and convert the change log to Changelog Stream.

The format of the data captured by delete events in change streams: [delete event](https://www.mongodb.com/docs/manual/reference/change-events/delete/)
```
{
   "_id": { <Resume Token> },
   "operationType": "delete",
   "clusterTime": <Timestamp>,
   "ns": {
      "db": "engineering",
      "coll": "users"
   },
   "documentKey": {
      "_id": ObjectId("599af247bb69cd89961c986d")
   }
}
```
The fullDocument document is omitted as the document no longer exists at the time the change stream cursor sends the delete event to the client.

## How to Create a MongoDB CDC Data Synchronization Jobs

### CDC Data Print to Client

The following example demonstrates how to create a data synchronization job that reads cdc data from MongoDB and prints it on the local client:

```hocon
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    username = stuser
    password = stpw
    schema = {
      table = "inventory.products"
      primaryKey {
        name = "id"
        columnNames = ["_id"]
      }
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

## CDC Data Write to MysqlDB

The following example demonstrates how to create a data synchronization job that reads cdc data from MongoDB and write to mysql database:

```hocon
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    username = stuser
    password = stpw
    schema = {
      table = "inventory.products"
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"

    generate_sink_sql = true
    # You need to configure both database and table
    database = mongodb_cdc
    table = products
    primary_keys = ["_id"]
  }
}
```

### CDC Data Write to Another MongoDB

You can also route CDC events to a sink MongoDB collection. The example below mirrors `inventory.products` from the source cluster to a target cluster named `mongo1`:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}

sink {
  MongoDB {
    uri = "mongodb://mongo1:27017"
    database = "inventory"
    collection = "products_mirror"
  }
}
```

## Startup From a Specific Timestamp

If you want to skip the snapshot and resume the change stream from a known point in time, set `startup.mode` to `timestamp` and provide `startup.timestamp` in epoch milliseconds. This is useful when re-processing a backlog of changes after a maintenance window or when bootstrapping a new sink that should ignore historical writes.

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    startup.mode = "timestamp"
    # 2026-08-01 00:00:00 UTC
    startup.timestamp = 1785542400000
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

## Using the SRV Connection URI

For MongoDB Atlas or any deployment that exposes a `mongodb+srv://` connection string, pass the URI directly to the `hosts` option. Authentication credentials, the replica set name, and other URI options are forwarded to the driver as-is, so you do not need to also set `connection.options`:

```hocon
source {
  MongoDB-CDC {
    hosts = "mongodb+srv://cluster0.example.net"
    username = "stuser"
    password = "stpw"
    database = ["inventory"]
    collection = ["inventory.products"]
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

## Heartbeat and Resume Token Maintenance

Change stream resume tokens can expire if no source records are published for a long time (for example, on a low-traffic collection). Set `heartbeat.interval.ms` to a non-zero value so the connector periodically advances the resume token and keeps the change stream open across checkpoint restores:

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    # Send a heartbeat every 30 seconds when no records are flowing
    heartbeat.interval.ms = 30000
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

## Reading From Multiple MongoDB Sources

A SeaTunnel job only accepts one source block per `source { ... }`, so the supported way to
fan in CDC streams from several MongoDB clusters is to submit one job per source and have them
write to the same sink table. Each job preserves its own parallelism, schema, and restart
tokens; the sink table deduplicates by primary key.

```hocon
# Job A: CDC from cluster `mongo0` writing to `inventory_a.products_a`
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory_a"]
    collection = ["inventory_a.products_a"]
    username = superuser
    password = superpw
    schema = {
      fields {
        "_id": string,
        "name": string,
        "price": int
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_e2e:3306/mongodb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"
    generate_sink_sql = true
    database = mongodb_cdc
    table = "${table_name}"
    primary_keys = ["_id"]
  }
}
```

```hocon
# Job B: CDC from cluster `mongo1` writing to `inventory_b.products_b`
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo1:27017"
    database = ["inventory_b"]
    collection = ["inventory_b.products_b"]
    username = superuser
    password = superpw
    schema = {
      fields {
        "_id": string,
        "name": string,
        "price": int
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_e2e:3306/mongodb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"
    generate_sink_sql = true
    database = mongodb_cdc
    table = "${table_name}"
    primary_keys = ["_id"]
  }
}
```

## Multi-table Synchronization

The following example demonstrates how to read CDC data from multiple MongoDB collections and write each collection to the matching MySQL table. The sink table uses `${table_name}`, so `inventory.products` and `inventory.orders` are routed to their own target tables.

```hocon
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products", "inventory.orders"]
    username = superuser
    password = superpw
    tables_configs = [
      {
        schema {
          table = "inventory.products"
          fields {
            "_id" : string,
            "name" : string,
            "description" : string,
            "weight" : string
          }
        }
      },
      {
        schema {
          table = "inventory.orders"
          fields {
            "_id" : string,
            "order_number" : int,
            "order_date" : string,
            "quantity" : int,
            "product_id" : string
          }
        }
      }
    ]
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mongodb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"
    generate_sink_sql = true
    database = mongodb_cdc
    table = "${table_name}"
    primary_keys = ["_id"]
  }
}
```

## CDC Metadata Fields

MongoDB CDC exposes metadata fields that can be used by the `Metadata` transform:

| Field | Type | Description |
|-------|------|-------------|
| database | STRING | Source database name. |
| table | STRING | Source collection name. |
| rowKind | STRING | Change type, such as insert, update, or delete. |
| ts_ms | LONG | Source event timestamp in milliseconds. |
| delay | LONG | Delay between event time and processing time in milliseconds. |

Example:

```hocon
transform {
  Metadata {
    metadata_fields {
      Database = database
      Table = table
      RowKind = rowKind
      EventTime = ts_ms
      Delay = delay
    }
  }
}
```

## Format of real-time streaming data

```shell
{
   _id : { <BSON Object> },        // Identifier of the open change stream, can be assigned to the 'resumeAfter' parameter for subsequent resumption of this change stream
   "operationType" : "<operation>",        // The type of change operation that occurred, such as: insert, delete, update, etc.
   "fullDocument" : { <document> },      // The full document data involved in the change operation. This field does not exist in delete operations
   "ns" : {   
      "db" : "<database>",         // The database where the change operation occurred
      "coll" : "<collection>"     // The collection where the change operation occurred
   },
   "to" : {   // These fields are displayed only when the operation type is 'rename'
      "db" : "<database>",         // The new database name after the change
      "coll" : "<collection>"     // The new collection name after the change
   },
   "source":{
        "ts_ms":"<timestamp>",     // The timestamp when the change operation occurred
        "table":"<collection>"     // The collection where the change operation occurred
        "db":"<database>",         // The database where the change operation occurred
        "snapshot":"false"         // Identify the current stage of data synchronization
    },
   "documentKey" : { "_id" : <value> },  // The _id field value of the document involved in the change operation
   "updateDescription" : {    // Description of the update operation
      "updatedFields" : { <document> },  // The fields and values that the update operation modified
      "removedFields" : [ "<field>", ... ]     // The fields and values that the update operation removed
   }
   "clusterTime" : <Timestamp>,     // The timestamp of the Oplog log entry corresponding to the change operation
   "txnNumber" : <NumberLong>,    // If the change operation is executed in a multi-document transaction, this field and value are displayed, representing the transaction number
   "lsid" : {          // Represents information related to the Session in which the transaction is located
      "id" : <UUID>,  
      "uid" : <BinData>
   }
}
```

## Changelog

<ChangeLog />

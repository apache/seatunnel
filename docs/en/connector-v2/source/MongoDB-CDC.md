# MongoDB CDC

> MongoDB CDC source connector

Support Those Engines
---------------------

> SeaTunnel Zeta<br/>

Key Features
------------

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

Description
-----------

The MongoDB CDC connector allows for reading snapshot data and incremental data from MongoDB database.

Supported DataSource Info
-------------------------

In order to use the Mongodb connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                    Dependency                                                     |
|------------|--------------------|-------------------------------------------------------------------------------------------------------------------|
| MongoDB    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-cdc-mongodb) |

Availability Settings
---------------------

1.MongoDB version: MongoDB version >= 4.0.

2.Cluster deployment: replica sets or sharded clusters.

3.Storage Engine: WiredTiger Storage Engine.

4.Permissions:changeStream and read

```shell
use admin;
db.createRole(
    {
        role: "flinkrole",
        privileges: [{
            resource: { db: "", collection: "" },
            actions: [
                "splitVector",
                "listDatabases",
                "listCollections",
                "collStats",
                "find",
                "changeStream" ]
        }],
        roles: [
            { role: 'read', db: 'config' }
        ]
    }
);

db.createUser(
  {
      user: 'flinkuser',
      pwd: 'flinkpw',
      roles: [
         { role: 'flinkrole', db: 'admin' }
      ]
  }
);
```

Data Type Mapping
-----------------

The following table lists the field data type mapping from MongoDB BSON type to Seatunnel data type.

| MongoDB BSON type | Seatunnel Data type |
|-------------------|---------------------|
| ObjectId          | STRING              |
| String            | STRING              |
| Boolean           | BOOLEAN             |
| Binary            | BINARY              |
| Int32             | INTEGER             |
| Int64             | BIGINT              |
| Double            | DOUBLE              |
| Decimal128        | DECIMAL             |
| Date              | Date                |
| Timestamp         | Timestamp           |
| Object            | ROW                 |
| Array             | ARRAY               |

For specific types in MongoDB, we use Extended JSON format to map them to Seatunnel STRING type.

| MongoDB BSON type |                                       Seatunnel STRING                                       |
|-------------------|----------------------------------------------------------------------------------------------|
| Symbol            | {"_value": {"$symbol": "12"}}                                                                |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}                       |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}}                                           |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**Tips**

> 1.When using the DECIMAL type in SeaTunnel, be aware that the maximum range cannot exceed 34 digits, which means you should use decimal(34, 18).<br/>

Source Options
--------------

|                Name                |  Type   | Required | Default |                                                                                                                                                                    Description                                                                                                                                                                     |
|------------------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hosts                              | String  | Yes      | -       | The comma-separated list of hostname and port pairs of the MongoDB servers. eg. `localhost:27017,localhost:27018`                                                                                                                                                                                                                                  |
| username                           | String  | No       | -       | Name of the database user to be used when connecting to MongoDB.                                                                                                                                                                                                                                                                                   |
| password                           | String  | No       | -       | Password to be used when connecting to MongoDB.                                                                                                                                                                                                                                                                                                    |
| databases                          | String  | Yes      | -       | Name of the database to watch for changes. If not set then all databases will be captured. The database also supports regular expressions to monitor multiple databases matching the regular expression. eg. `db1,db2`                                                                                                                             |
| collections                        | String  | Yes      | -       | Name of the collection in the database to watch for changes. If not set then all collections will be captured. The collection also supports regular expressions to monitor multiple collections matching fully-qualified collection identifiers. eg. `db1.coll1,db2.coll2`                                                                         |
| connection.options                 | String  | No       | -       | The ampersand-separated connection options of MongoDB.  eg. `replicaSet=test&connectTimeoutMS=300000`                                                                                                                                                                                                                                              |
| full-incremental.integration       | Boolean | No       | true    | By using the `full-incremental.integration` configuration option, you can decide whether to obtain a snapshot of the entire collection based on your needs. If you want to retrieve the complete dataset upon startup, you can set it to true. If you are only interested in changes after startup, you can keep it at the default value of false. |
| full-incremental.queue.size        | Integer | No       | 10240   | The max size of the queue to use when full-incremental integration mode.                                                                                                                                                                                                                                                                           |
| batch.size                         | Long    | No       | 1024    | The cursor batch size.                                                                                                                                                                                                                                                                                                                             |
| poll.max.batch.size                | Enum    | No       | 1024    | Maximum number of change stream documents to include in a single batch when polling for new data.                                                                                                                                                                                                                                                  |
| poll.await.time.ms                 | Long    | No       | 1000    | The amount of time to wait before checking for new results on the change stream.                                                                                                                                                                                                                                                                   |
| heartbeat.interval.ms              | String  | No       | 0       | The length of time in milliseconds between sending heartbeat messages. Use 0 to disable.                                                                                                                                                                                                                                                           |
| incremental.snapshot.chunk.size.mb | Long    | No       | 64      | The chunk size mb of incremental snapshot.                                                                                                                                                                                                                                                                                                         |

**Tips:**
1.If the collection changes at a slow pace, it is strongly recommended to set an appropriate value greater than 0 for the heartbeat.interval.ms parameter. When we recover a Seatunnel job from a checkpoint or savepoint, the heartbeat events can push the resumeToken forward to avoid its expiration.

#### example

```conf
source {
  MongoDB-CDC {
  }
}
```

## Example

```Jdbc {
source {
  MongoDB-CDC {
  }
}
```

## Changelog

- Add MongoDB CDC Source Connector

### next version


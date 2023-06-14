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
        role: "strole",
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
      user: 'stuser',
      pwd: 'stpw',
      roles: [
         { role: 'strole', db: 'admin' }
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

|                Name                |  Type  | Required | Default |                                                                                                                                Description                                                                                                                                 |
|------------------------------------|--------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hosts                              | String | Yes      | -       | The comma-separated list of hostname and port pairs of the MongoDB servers. eg. `localhost:27017,localhost:27018`                                                                                                                                                          |
| username                           | String | No       | -       | Name of the database user to be used when connecting to MongoDB.                                                                                                                                                                                                           |
| password                           | String | No       | -       | Password to be used when connecting to MongoDB.                                                                                                                                                                                                                            |
| database                           | String | Yes      | -       | Name of the database to watch for changes. If not set then all databases will be captured. The database also supports regular expressions to monitor multiple databases matching the regular expression. eg. `db1,db2`                                                     |
| collection                         | String | Yes      | -       | Name of the collection in the database to watch for changes. If not set then all collections will be captured. The collection also supports regular expressions to monitor multiple collections matching fully-qualified collection identifiers. eg. `db1.coll1,db2.coll2` |
| connection.options                 | String | No       | -       | The ampersand-separated connection options of MongoDB.  eg. `replicaSet=test&connectTimeoutMS=300000`                                                                                                                                                                      |
| batch.size                         | Long   | No       | 1024    | The cursor batch size.                                                                                                                                                                                                                                                     |
| poll.max.batch.size                | Enum   | No       | 1024    | Maximum number of change stream documents to include in a single batch when polling for new data.                                                                                                                                                                          |
| poll.await.time.ms                 | Long   | No       | 1000    | The amount of time to wait before checking for new results on the change stream.                                                                                                                                                                                           |
| heartbeat.interval.ms              | String | No       | 0       | The length of time in milliseconds between sending heartbeat messages. Use 0 to disable.                                                                                                                                                                                   |
| incremental.snapshot.chunk.size.mb | Long   | No       | 64      | The chunk size mb of incremental snapshot.                                                                                                                                                                                                                                 |

**Tips:**

> 1.If the collection changes at a slow pace, it is strongly recommended to set an appropriate value greater than 0 for the heartbeat.interval.ms parameter. When we recover a Seatunnel job from a checkpoint or savepoint, the heartbeat events can push the resumeToken forward to avoid its expiration.<br/>
> 2.MongoDB has a limit of 16MB for a single document. Change documents include additional information, so even if the original document is not larger than 15MB, the change document may exceed the 16MB limit, resulting in the termination of the Change Stream operation.<br/>
> 3.It is recommended to use immutable shard keys. In MongoDB, shard keys allow modifications after transactions are enabled, but changing the shard key can cause frequent shard migrations, resulting in additional performance overhead. Additionally, modifying the shard key can also cause the Update Lookup feature to become ineffective, leading to inconsistent results in CDC (Change Data Capture) scenarios.<br/>

#### example

```conf
env {
  # You can set engine configuration here
  execution.parallelism = 1
  job.mode = "STREAMING"
  execution.checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = "inventory"
    collection = "inventory.products"
    username = stuser
    password = stpw
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
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user"
    password = "seatunnel"

    generate_sink_sql = true
    # You need to configure both database and table
    database = mongodb_cdc
    table = products
    primary_keys = ["_id"]
  }
}
```

## Changelog

- Add MongoDB CDC Source Connector

### next version


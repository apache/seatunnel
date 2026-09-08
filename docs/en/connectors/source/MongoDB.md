import ChangeLog from '../changelog/connector-mongodb.md';

# MongoDB

> MongoDB Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The MongoDB source connector reads documents from a MongoDB collection and converts each BSON
document into a SeaTunnel row. It supports both **batch** and **streaming** jobs, and reads in
parallel by splitting the source collection across `partition.split-key` ranges.

You can narrow what is read and what columns are returned without scanning the whole collection:

- Use `match.query` to filter documents by a MongoDB query expression.
- Use `match.projection` to control which fields appear in the result.
- Use `flat.sync-string` to capture the whole document as one JSON `STRING` column when no
  fixed schema is needed.

In streaming mode the connector reads the assigned splits and tracks progress through checkpoints
so a restarted job resumes from the last committed cursor.

## Supported DataSource Info

In order to use the MongoDB connector, the following dependency is required.
It can be downloaded via `install-plugin.sh` or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                            |
|------------|--------------------|---------------------------------------------------------------------------------------|
| MongoDB    | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-mongodb) |

## Data Type Mapping

The following table lists the field data type mapping from MongoDB BSON type to SeaTunnel data type.

| MongoDB BSON type | SeaTunnel Data Type |
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

For specific types in MongoDB, the connector uses Extended JSON format and maps them to the
SeaTunnel `STRING` type.

| MongoDB BSON type |                                       SeaTunnel STRING                                       |
|-------------------|----------------------------------------------------------------------------------------------|
| Symbol            | {"_value": {"$symbol": "12"}}                                                                |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}                       |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}}                                           |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**Tips**

> 1. When using the `DECIMAL` type in SeaTunnel, the maximum range cannot exceed 34 digits. Use
>    `decimal(34, 18)` to stay within the supported precision and scale.

## Source Options

| Name                  | Type    | Required | Default            | Description                                                                                                                                                                                                                                                                            |
|-----------------------|---------|----------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                   | String  | Yes      | -                  | The MongoDB standard connection URI, for example `mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true`. See [Parameter Interpretation](#parameter-interpretation) for more URI samples.                                                                       |
| database              | String  | Yes      | -                  | The name of the MongoDB database to read from.                                                                                                                                                                                                                                         |
| collection            | String  | Yes      | -                  | The name of the MongoDB collection to read from.                                                                                                                                                                                                                                       |
| schema                | Config  | Yes      | -                  | The mapping between MongoDB's BSON and SeaTunnel data structure. For more details, see [Schema Feature](../../introduction/concepts/schema-feature.md).                                                                                                                                  |
| match.query           | String  | No       | -                  | MongoDB query expression used to filter documents for read operations. Compatible with the legacy option name `matchQuery`.                                                                                                                                                             |
| match.projection      | String  | No       | -                  | MongoDB projection expression used to control which fields appear in the result.                                                                                                                                                                                                        |
| partition.split-key   | String  | No       | _id                | The field used as the MongoDB split key. The connector splits the collection by the value range of this key.                                                                                                                                                                            |
| partition.split-size  | Long    | No       | 64 * 1024 * 1024   | The size of each MongoDB split. Smaller split sizes increase the number of splits and parallelism, while larger sizes reduce it.                                                                                                                                                        |
| cursor.no-timeout     | Boolean | No       | true               | MongoDB server normally times out idle cursors after 10 minutes of inactivity to reclaim memory. Set this option to `true` to keep the cursor open across long-running batches. If the application holds the batch for more than 30 minutes, MongoDB marks the session as expired and closes it.   |
| fetch.size            | Int     | No       | 2048               | The number of documents obtained from the server per batch. Tuning this value balances query performance and memory pressure.                                                                                                                                                          |
| max.time-min          | Long    | No       | 10                 | The maximum execution time (in minutes) for each MongoDB query. MongoDB terminates the operation and returns an error when this limit is exceeded.                                                                                                                                      |
| flat.sync-string      | Boolean | No       | false              | When enabled, the connector maps the whole MongoDB document into one SeaTunnel `STRING` field. The schema must contain exactly one field and that field must be of type `STRING`.                                                                                                        |
| common-options        |         | No       | -                  | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                                                      |

### Tips

> 1. The `match.query` option is compatible with the legacy option name `matchQuery`; they are equivalent.
> 2. Use `partition.split-key` together with `partition.split-size` to control parallel reads. The
>    split key should reference an indexed field for best performance.
> 3. When `flat.sync-string = true`, the configured schema is ignored except for the single `STRING`
>    field that receives the document.

## How to Create a MongoDB Data Synchronization Job

The following example reads data from MongoDB and prints it on the local client:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "source_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(34, 18)"
        c_timestamp = timestamp
        c_row = {
          c_map = "map<string, string>"
          c_array = "array<int>"
          c_string = string
          c_boolean = boolean
          c_int = int
          c_bigint = bigint
          c_double = double
          c_bytes = bytes
          c_date = date
          c_decimal = "decimal(34, 18)"
          c_timestamp = timestamp
        }
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

## Parameter Interpretation

### MongoDB Database Connection URI Examples

Unauthenticated single node connection:

```bash
mongodb://192.168.0.100:27017/mydb
```

Replica set connection:

```bash
mongodb://192.168.0.100:27017/mydb?replicaSet=xxx
```

Authenticated replica set connection:

```bash
mongodb://admin:password@192.168.0.100:27017/mydb?replicaSet=xxx&authSource=admin
```

Multi-node replica set connection:

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb?replicaSet=xxx
```

Sharded cluster connection (route through one `mongos`):

```bash
mongodb://mongos1.example.com:27017,mongos2.example.com:27017,mongos3.example.com:27017/mydb
```

Multiple mongos connections (comma-separated list of mongos hosts):

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb
```

> Note: The username and password in the URI must be URL-encoded before being concatenated into
> the connection string.

### MatchQuery Scan

In data synchronization scenarios, the `match.query` approach should be used early to reduce the
number of documents that need to be processed by downstream operators, improving overall
performance. Here is a simple example of using `match.query`:

```hocon
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "orders"
    match.query = "{status: \"A\"}"
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}
```

The following are examples of `match.query` expressions for various data types:

```bash
# Query Boolean type
"{c_boolean: true}"
# Query string type
"{c_string: \"OCzCj\"}"
# Query the integer
"{c_int: 2}"
# Query the date type
"{c_date: {\$date: \"2023-06-26T16:00:00.000Z\"}}"
# Query the floating point type
"{c_double: {\$gte: 1.71763202185342e+308}}"
```

Refer to the MongoDB manual for the full query syntax:
<https://www.mongodb.com/docs/manual/tutorial/query-documents>

### Projection Scan

In MongoDB, projection controls which fields appear in the query results by specifying which
fields are returned and which are excluded. In the `find()` method, a projection object can be
passed as the second argument. A value of `1` includes the field, `0` excludes it. For example,
given a `users` collection:

```javascript
// Returns only the `name` field and excludes the `email` field
db.users.find({}, { name: 1, email: 0 });
```

In data synchronization scenarios, projection should be used early to reduce the number of fields
that need to be processed by downstream operators. Here is a simple example of using projection in
SeaTunnel:

```hocon
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    match.projection = "{ name: 1, email: 0 }"
    schema = {
      fields {
        name = string
      }
    }
  }
}
```

### Partitioned Scan

To speed up reading data in parallel source tasks, SeaTunnel provides a partitioned scan feature
for MongoDB collections. Configure `partition.split-key` for the split field and
`partition.split-size` for the split size to control data sharding:

```hocon
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    partition.split-key = "id"
    partition.split-size = 1024
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}
```

> Tip: Pick a split key backed by an index so the source can fast-skip through the collection when
> enumerating split ranges.

### Flat Sync String

By enabling `flat.sync-string`, you only need to declare a single field whose type is `STRING`. The
connector will serialize each MongoDB document as an Extended JSON string and put it into that
field.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    flat.sync-string = true
    schema = {
      fields {
        data = string
      }
    }
  }
}
sink {
  Console {}
}
```

A sample document that flows through this configuration looks like the following:

```json
{
  "_id": {
    "$oid": "643d41f5fdc6a52e90e59cbf"
  },
  "c_map": {
    "OQBqH": "jllt",
    "rkvlO": "pbfdf",
    "pCMEX": "hczrdtve",
    "DAgdj": "t",
    "dsJag": "voo"
  },
  "c_array": [
    { "$numberInt": "-865590937" },
    { "$numberInt": "833905600" },
    { "$numberInt": "-1104586446" },
    { "$numberInt": "2076336780" },
    { "$numberInt": "-1028686444" }
  ],
  "c_string": "bddkzxr",
  "c_boolean": false,
  "c_tinyint": { "$numberInt": "39" },
  "c_smallint": { "$numberInt": "23672" },
  "c_int": { "$numberInt": "-495763561" },
  "c_bigint": { "$numberLong": "3768307617923954543" },
  "c_double": { "$numberDouble": "1.1706091642478246E308" },
  "c_bytes": { "$binary": { "base64": "ZWJ4", "subType": "00" } },
  "c_date": { "$date": { "$numberLong": "1686614400000" } },
  "c_decimal": { "$numberDecimal": "683265300" },
  "c_timestamp": { "$date": { "$numberLong": "1684283772000" } }
}
```

## Changelog

<ChangeLog />
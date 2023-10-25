# MongoDB

> MongoDB Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

The MongoDB Connector provides the ability to read and write data from and to MongoDB.
This document describes how to set up the MongoDB connector to run data reads against MongoDB.

## Supported DataSource Info

In order to use the Mongodb connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                  Dependency                                                   |
|------------|--------------------|---------------------------------------------------------------------------------------------------------------|
| MongoDB    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-mongodb) |

## Data Type Mapping

The following table lists the field data type mapping from MongoDB BSON type to SeaTunnel data type.

| MongoDB BSON type | SeaTunnel Data type |
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

For specific types in MongoDB, we use Extended JSON format to map them to SeaTunnel STRING type.

| MongoDB BSON type |                                       SeaTunnel STRING                                       |
|-------------------|----------------------------------------------------------------------------------------------|
| Symbol            | {"_value": {"$symbol": "12"}}                                                                |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}                       |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}}                                           |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**Tips**

> 1.When using the DECIMAL type in SeaTunnel, be aware that the maximum range cannot exceed 34 digits, which means you should use decimal(34, 18).<br/>

## Source Options

|         Name         |  Type   | Required |     Default      |                                                                                                                                                  Description                                                                                                                                                   |
|----------------------|---------|----------|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                  | String  | Yes      | -                | The MongoDB standard connection uri. eg. mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true.                                                                                                                                                                                   |
| database             | String  | Yes      | -                | The name of MongoDB database to read or write.                                                                                                                                                                                                                                                                 |
| collection           | String  | Yes      | -                | The name of MongoDB collection to read or write.                                                                                                                                                                                                                                                               |
| schema               | String  | Yes      | -                | MongoDB's BSON and seatunnel data structure mapping.                                                                                                                                                                                                                                                           |
| match.query          | String  | No       | -                | In MongoDB, filters are used to filter documents for query operations.                                                                                                                                                                                                                                         |
| match.projection     | String  | No       | -                | In MongoDB, Projection is used to control the fields contained in the query results.                                                                                                                                                                                                                           |
| partition.split-key  | String  | No       | _id              | The key of Mongodb fragmentation.                                                                                                                                                                                                                                                                              |
| partition.split-size | Long    | No       | 64 * 1024 * 1024 | The size of Mongodb fragment.                                                                                                                                                                                                                                                                                  |
| cursor.no-timeout    | Boolean | No       | true             | MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to true to prevent that. However, if the application takes longer than 30 minutes to process the current batch of documents, the session is marked as expired and closed. |
| fetch.size           | Int     | No       | 2048             | Set the number of documents obtained from the server for each batch. Setting the appropriate batch size can improve query performance and avoid the memory pressure caused by obtaining a large amount of data at one time.                                                                                    |
| max.time-min         | Long    | No       | 600              | This parameter is a MongoDB query option that limits the maximum execution time for query operations. The value of maxTimeMin is in Minute. If the execution time of the query exceeds the specified time limit, MongoDB will terminate the operation and return an error.                                     |
| flat.sync-string     | Boolean | No       | true             | By utilizing flatSyncString, only one field attribute value can be set, and the field type must be a String. This operation will perform a string mapping on a single MongoDB data entry.                                                                                                                      |
| common-options       |         | No       | -                | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                                                                        |

### Tips

> 1.The parameter `match.query` is compatible with the historical old version parameter `matchQuery`, and they are equivalent replacements.<br/>

## How to Create a MongoDB Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from MongoDB and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to Mongodb
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
        c_decimal = "decimal(38, 18)"
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
          c_decimal = "decimal(38, 18)"
          c_timestamp = timestamp
        }
      }
    }
  }
}

# Console printing of the read Mongodb data
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

Sharded cluster connection:

```bash
mongodb://192.168.0.100:27017/mydb
```

Multiple mongos connections:

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb
```

Note: The username and password in the URI must be URL-encoded before being concatenated into the connection string.

### MatchQuery Scan

In data synchronization scenarios, the matchQuery approach needs to be used early to reduce the number of documents that need to be processed by subsequent operators, thus improving performance.
Here is a simple example of a seatunnel using `match.query`

```bash
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

The following are examples of MatchQuery query statements of various data types:

```bash
# Query Boolean type
"{c_boolean:true}"
# Query string type
"{c_string:\"OCzCj\"}"
# Query the integer
"{c_int:2}"
# Type of query time
"{c_date:ISODate(\"2023-06-26T16:00:00.000Z\")}"
# Query floating point type
{c_double:{$gte:1.71763202185342e+308}}
```

Please refer to how to write the syntax of `match.query`：https://www.mongodb.com/docs/manual/tutorial/query-documents

### Projection Scan

In MongoDB, Projection is used to control which fields are included in the query results. This can be accomplished by specifying which fields need to be returned and which fields do not.
In the find() method, a projection object can be passed as a second argument. The key of the projection object indicates the fields to include or exclude, and a value of 1 indicates inclusion and 0 indicates exclusion.
Here is a simple example, assuming we have a collection named users:

```bash
# Returns only the name and email fields
db.users.find({}, { name: 1, email: 0 });
```

In data synchronization scenarios, projection needs to be used early to reduce the number of documents that need to be processed by subsequent operators, thus improving performance.
Here is a simple example of a seatunnel using projection:

```bash
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

To speed up reading data in parallel source task instances, seatunnel provides a partitioned scan feature for MongoDB collections. The following partitioning strategies are provided.
Users can control data sharding by setting the partition.split-key for sharding keys and partition.split-size for sharding size.

```bash
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

### Flat Sync String

By utilizing `flat.sync-string`, only one field attribute value can be set, and the field type must be a String.
This operation will perform a string mapping on a single MongoDB data entry.

```bash
env {
  execution.parallelism = 10
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

Use the data samples synchronized with modified parameters, such as the following:

```json
{
  "_id":{
    "$oid":"643d41f5fdc6a52e90e59cbf"
  },
  "c_map":{
    "OQBqH":"jllt",
    "rkvlO":"pbfdf",
    "pCMEX":"hczrdtve",
    "DAgdj":"t",
    "dsJag":"voo"
  },
  "c_array":[
    {
      "$numberInt":"-865590937"
    },
    {
      "$numberInt":"833905600"
    },
    {
      "$numberInt":"-1104586446"
    },
    {
      "$numberInt":"2076336780"
    },
    {
      "$numberInt":"-1028688944"
    }
  ],
  "c_string":"bddkzxr",
  "c_boolean":false,
  "c_tinyint":{
    "$numberInt":"39"
  },
  "c_smallint":{
    "$numberInt":"23672"
  },
  "c_int":{
    "$numberInt":"-495763561"
  },
  "c_bigint":{
    "$numberLong":"3768307617923954543"
  },
  "c_float":{
    "$numberDouble":"5.284220288280258E37"
  },
  "c_double":{
    "$numberDouble":"1.1706091642478246E308"
  },
  "c_bytes":{
    "$binary":{
      "base64":"ZWJ4",
      "subType":"00"
    }
  },
  "c_date":{
    "$date":{
      "$numberLong":"1686614400000"
    }
  },
  "c_decimal":{
    "$numberDecimal":"683265300"
  },
  "c_timestamp":{
    "$date":{
      "$numberLong":"1684283772000"
    }
  },
  "c_row":{
    "c_map":{
      "OQBqH":"cbrzhsktmm",
      "rkvlO":"qtaov",
      "pCMEX":"tuq",
      "DAgdj":"jzop",
      "dsJag":"vwqyxtt"
    },
    "c_array":[
      {
        "$numberInt":"1733526799"
      },
      {
        "$numberInt":"-971483501"
      },
      {
        "$numberInt":"-1716160960"
      },
      {
        "$numberInt":"-919976360"
      },
      {
        "$numberInt":"727499700"
      }
    ],
    "c_string":"oboislr",
    "c_boolean":true,
    "c_tinyint":{
      "$numberInt":"-66"
    },
    "c_smallint":{
      "$numberInt":"1308"
    },
    "c_int":{
      "$numberInt":"-1573886733"
    },
    "c_bigint":{
      "$numberLong":"4877994302999518682"
    },
    "c_float":{
      "$numberDouble":"1.5353209063652051E38"
    },
    "c_double":{
      "$numberDouble":"1.1952441956458565E308"
    },
    "c_bytes":{
      "$binary":{
        "base64":"cWx5Ymp0Yw==",
        "subType":"00"
      }
    },
    "c_date":{
      "$date":{
        "$numberLong":"1686614400000"
      }
    },
    "c_decimal":{
      "$numberDecimal":"656406177"
    },
    "c_timestamp":{
      "$date":{
        "$numberLong":"1684283772000"
      }
    }
  },
  "id":{
    "$numberInt":"2"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add MongoDB Source Connector

### Next Version

- [Feature]Refactor mongodb source connector([4620](https://github.com/apache/seatunnel/pull/4620))


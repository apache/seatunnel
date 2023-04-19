# MongoDB

> MongoDB source connector

The MongoDB Connector provides the ability to read and write data from and to MongoDB.
This document describes how to set up the MongoDB connector to run data reads against MongoDB.

Support those engines
---------------------

> Spark
> Flink
> SeaTunnel Zeta

Key featuresl
-------------

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

Dependencies
------------

In order to use the Mongodb connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| MongoDB version |                                                  dependency                                                   |
|-----------------|---------------------------------------------------------------------------------------------------------------|
| universal       | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-mongodb) |

Data Type Mapping
-----------------

The following table lists the field data type mapping from MongoDB BSON type to Seatunnel data type.

| MongoDB BSON type | Seatunnel type |
|-------------------|----------------|
| ObjectId          | STRING         |
| String            | STRING         |
| Boolean           | BOOLEAN        |
| Binary            | BINARY         |
| Int32             | INTEGER        |
| -                 | TINYINT        |
| -                 | SMALLINT       |
| -                 | BIGINT         |
| Double            | DOUBLE         |
| -                 | FLOAT          |
| Decimal128        | DECIMAL        |
| Date              | Date           |
| -                 | TIME           |
| Timestamp         | Timestamp      |
| Object            | ROW            |
| Array             | ARRAY          |

tips：
1.When using the DECIMAL type in SeaTunnel, be aware that the maximum range cannot exceed 34 digits, which means you should use decimal(34, 18).

Connector Options
-----------------

|        Option        | Required |      Default      |  Type   |                                                                                                                                                  Description                                                                                                                                                   |
|----------------------|----------|-------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector            | required | (none)            | String  | The MongoDB connection uri.                                                                                                                                                                                                                                                                                    |
| database             | required | (none)            | String  | The name of MongoDB database to read or write.                                                                                                                                                                                                                                                                 |
| collection           | required | (none)            | String  | The name of MongoDB collection to read or write.                                                                                                                                                                                                                                                               |
| schema               | required | (none)            | String  | MongoDB's BSON and seatunnel data structure mapping                                                                                                                                                                                                                                                            |
| match.query          | optional | (none)            | String  | In MongoDB, $match is one of the aggregation pipeline operators, used to filter documents                                                                                                                                                                                                                      |
| match.projection     | optional | (none)            | String  | In MongoDB, Projection is used to control the fields contained in the query results                                                                                                                                                                                                                            |
| partition.split-key  | optional | _id               | String  | The key of Mongodb fragmentation.                                                                                                                                                                                                                                                                              |
| partition.split-size | optional | 64 * 1024 * 1024L | Long    | The size of Mongodb fragment.                                                                                                                                                                                                                                                                                  |
| no-timeout           | optional | true              | Boolean | MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to true to prevent that. However, if the application takes longer than 30 minutes to process the current batch of documents, the session is marked as expired and closed. |
| fetch.size           | optional | 2048              | Int     | Set the number of documents obtained from the server for each batch. Setting the appropriate batch size can improve query performance and avoid the memory pressure caused by obtaining a large amount of data at one time.                                                                                    |
| max.time.min         | optional | 600L              | Long    | This parameter is a MongoDB query option that limits the maximum execution time for query operations. The value of maxTimeMS is in milliseconds. If the execution time of the query exceeds the specified time limit, MongoDB will terminate the operation and return an error.                                |

How to create a MongoDB Data synchronization jobs
-------------------------------------------------

The example below shows how to create a MongoDB data synchronization jobs:

```bash
-- Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

-- Create a source to connect to Mongodb
source {
  MongodbV2 {
    connection = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "source_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
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
          c_tinyint = tinyint
          c_smallint = smallint
          c_int = int
          c_bigint = bigint
          c_float = float
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

-- Console printing of the read Mongodb data


}
```

Parameter interpretation
------------------------

**MatchQuery Scan**

In MongoDB, $match is one of the aggregation pipeline operators used to filter documents. Its position in the pipeline determines when documents are filtered.
$match uses MongoDB's standard query operators to filter data. Basically, it can be thought of as the "WHERE" clause in the aggregation pipeline.

Here's a simple $match example, assuming we have a collection called orders and want to filter out documents that meet the status field value of "A":

```bash
db.orders.aggregate([
  {
    $match: {
      status: "A"
    }
  }
]);

```

In data synchronization scenarios, the matchQuery approach needs to be used early to reduce the number of documents that need to be processed by subsequent operators, thus improving performance.
Here is a simple example of a seatunnel using $match

```bash
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "orders"
    match.query = "{
      status: "A"
    }"
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}
```

**Projection Scan**

In MongoDB, Projection is used to control which fields are included in the query results. This can be accomplished by specifying which fields need to be returned and which fields do not.
In the find() method, a projection object can be passed as a second argument. The key of the projection object indicates the fields to include or exclude, and a value of 1 indicates inclusion and 0 indicates exclusion.
Here is a simple example, assuming we have a collection named users:

```bash
// Returns only the name and email fields
db.users.find({}, { name: 1, email: 1 });
```

In data synchronization scenarios, projection needs to be used early to reduce the number of documents that need to be processed by subsequent operators, thus improving performance.
Here is a simple example of a seatunnel using projection:

```bash
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    match.projection = "{ name: 1, email: 1 }"
    schema = {
      fields {
        id = bigint
        status = string
        name = string
        email = string
      }
    }
  }
}

```

**Partitioned Scan**
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

## Changelog

### 2.2.0-beta 2022-09-26

- Add MongoDB Source Connector

### Next Version

- [Feature]Refactor mongodb source connector([4380](https://github.com/apache/incubator-seatunnel/pull/4380))


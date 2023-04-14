# Apache MongoDB connector

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

The MongoDB Connector provides the ability to read and write data from and to MongoDB.
This document describes how to set up the MongoDB connector to run data reads against MongoDB.

Dependencies
------------

In order to use the Mongodb connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| MongoDB version |                                                    dependency                                                    |
|-----------------|------------------------------------------------------------------------------------------------------------------|
| universal       | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-mongodb-v2) |

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

Connector Options
-----------------

|       Option       | Required | Default |    Type    |                                                                                                                                                                                                                  Description                                                                                                                                                                                                                  |
|--------------------|----------|---------|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector          | required | (none)  | String     | The MongoDB connection uri.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| database           | required | (none)  | String     | The name of MongoDB database to read or write.                                                                                                                                                                                                                                                                                                                                                                                                |
| collection         | required | (none)  | String     | The name of MongoDB collection to read or write.                                                                                                                                                                                                                                                                                                                                                                                              |
| schema             | required | (none)  | String     | MongoDB's BSON and seatunnel data structure mapping                                                                                                                                                                                                                                                                                                                                                                                           |
| match-query        | optional | (none)  | String     | In MongoDB, $match is one of the aggregation pipeline operators, used to filter documents                                                                                                                                                                                                                                                                                                                                                     |
| projection         | optional | (none)  | String     | In MongoDB, Projection is used to control the fields contained in the query results                                                                                                                                                                                                                                                                                                                                                           |
| partition.strategy | optional | default | String     | Specifies the partition strategy. Available strategies are `single`, `sample`, `split-vector`, `sharded` and `default`. See the following Partitioned Scan section for more details.                                                                                                                                                                                                                                                          |
| partition.size     | optional | 64mb    | MemorySize | Specifies the partition memory size.                                                                                                                                                                                                                                                                                                                                                                                                          |
| partition.samples  | optional | 10      | Integer    | Specifies the samples count per partition. It only takes effect when the partition strategy is sample. The sample partitioner samples the collection, projects and sorts by the partition fields. Then uses every `scan.partition.samples` as the value to use to calculate the partition boundaries. The total number of samples taken is calculated as: `samples per partition * (count of documents / number of documents per partition)`. |
| no-timeout         | optional | true    | Boolean    | MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to true to prevent that. However, if the application takes longer than 30 minutes to process the current batch of documents, the session is marked as expired and closed.                                                                                                                                |

Features
--------

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
    matchQuery = "{
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
    matchQuery = "{ name: 1, email: 1 }"
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}

```

**Partitioned Scan**
To speed up reading data in parallel source task instances, seatunnel provides a partitioned scan feature for MongoDB collections. The following partitioning strategies are provided.
- single: treats the entire collection as a single partition.
- sample: samples the collection and generate partitions which is fast but possibly uneven.
- split-vector: uses the splitVector command to generate partitions for non-sharded collections which is fast and even. The splitVector permission is required.
- sharded: reads config.chunks (MongoDB splits a sharded collection into chunks, and the range of the chunks are stored within the collection) as the partitions directly. The sharded strategy only used for sharded collection which is fast and even. Read permission of config database is required.
- default: uses sharded strategy for sharded collections otherwise using split vector strategy.

Data Type Mapping
-----------------

The following table lists the field data type mapping from MongoDB BSON type to Seatunnel data type.

| MongoDB BSON type |  Seatunnel type  |
|-------------------|------------------|
| ObjectId          | STRING           |
| String            | STRING           |
| Boolean           | BOOLEAN          |
| Binary            | BINARY           |
| Int32             | INTEGER          |
| Int64             | BIGINT           |
| Double            | DOUBLE           |
| Decimal128        | DECIMAL          |
| DateTime          | TIMESTAMP_LTZ(3) |
| Timestamp         | TIMESTAMP_LTZ(0) |
| Object            | ROW              |
| Array             | ARRAY            |


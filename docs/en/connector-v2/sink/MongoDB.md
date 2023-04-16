# MongoDB

> MongoDB sink connector

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
- [x] [exactly-once](../../concept/connector-v2-features.md)
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
| Null              | Null           |
| Boolean           | BOOLEAN        |
| Binary Data       | BINARY         |
| Int32             | INTEGER        |
| Int64             | BIGINT         |
| Double            | DOUBLE         |
| Decimal128        | DECIMAL        |
| Date              | Date           |
| DateTime          | TIME           |
| Timestamp         | Timestamp      |
| Object            | ROW            |
| Array             | ARRAY          |

Connector Options
-----------------

|        Option         | Required | Default |     Type     |                                            Description                                            |
|-----------------------|----------|---------|--------------|---------------------------------------------------------------------------------------------------|
| connector             | required | (none)  | String       | The MongoDB connection uri.                                                                       |
| database              | required | (none)  | String       | The name of MongoDB database to read or write.                                                    |
| collection            | required | (none)  | String       | The name of MongoDB collection to read or write.                                                  |
| schema                | required | (none)  | String       | MongoDB's BSON and seatunnel data structure mapping                                               |
| buffer-flush.max-rows | optional | 1000    | String       | Specifies the maximum number of buffered rows per batch request.                                  |
| buffer-flush.interval | optional | 30_000L | String       | Specifies the retry time interval if writing records to database failed, the unit is seconds.     |
| retry.max             | optional | default | String       | Specifies the max retry times if writing records to database failed.                              |
| retry.interval        | optional | 1000L   | Duration     | Specifies the retry time interval if writing records to database failed, the unit is millisecond. |
| is.exactly-once       | optional | false   | Boolean      | Implement the write semantics of only one time (exactly-once).                                    |
| upsert-enable         | optional | false   | Boolean      | Whether to write documents via upsert mode.                                                       |
| upsert-key            | optional | (none)  | List<String> | The primary keys for upsert. Only valid in upsert mode. Keys are in csv format for properties.    |

How to create a MongoDB Data synchronization jobs
-------------------------------------------------

The example below shows how to create a MongoDB data synchronization jobs:

```bash
# Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval  = 1000
}

source {
  FakeSource {
      row.num = 2
      bigint.min = 0
      bigint.max = 10000000
      split.num = 1
      split.read-interval = 300
      schema {
        fields {
          c_bigint = bigint
        }
      }
    }
}

sink {
  Mongodb{
    connection = "mongodb://localhost:27017/test"
    database = "test"
    collection = "temp"
    schema = {
      fields {
        c_bigint = bigint
        }
    }
  }
}


}
```

Parameter interpretation
------------------------

**Buffer Flush**

```bash
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    buffer-flush.max-rows = 2000
    buffer-flush.interval = 1000
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}
```

**Upsert**

```bash
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    upsert-enable = true
    upsert-key = id
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}
```

**Exactly Once**

when implementing Exactly Once write semantics with Flink or other stream processing frameworks, MongoDB Sink needs to ensure data consistency.
This means that during checkpoints, the MongodbWriter will confirm that all cached write operations have been correctly written through MongoDB's transaction commit mechanism.

```bash
env {
  execution.parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval  = 1000
}

sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    is.exactly-once = true
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


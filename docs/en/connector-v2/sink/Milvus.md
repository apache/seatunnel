> Milvus sink connector

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Description

Write data to Apache milvus.

## Sink Options

|       Name        |  Type  | Required |        Default         |                                  Description                                  |
|-------------------|--------|----------|------------------------|-------------------------------------------------------------------------------|
| milvus_host       | String | Yes      | -                      | The milvus host.                                                              |
| milvus_port       | Int    | No       | 19530                  | This port is for gRPC. Default is 19530.                                      |
| username          | String | Yes      | -                      | The username of milvus server.                                                |
| password          | String | Yes      | -                      | The password of milvus server.                                                |
| collection_name   | String | No       | -                      | A collection of milvus, which is similar to a table in a relational database. |
| partition_field   | String | No       | -                      | Partition fields, which must be included in the collection's schema.          |
| openai_engine     | String | No       | text-embedding-ada-002 | Text embedding model. Default is 'text-embedding-ada-002'.                    |
| openai_api_key    | String | No       | -                      | Use your own Open AI API Key here.                                            |
| embeddings_fields | String | No       | -                      | Fields to be embedded,They use`,`for splitting.                               |

### Data Type Mapping

| Milvus Data type | SeaTunnel Data type |
|------------------|---------------------|
| Bool             | BOOLEAN             |
| Int8             | TINYINT             |
| Int16            | SMALLINT            |
| Int32            | INT                 |
| Int64            | BIGINT              |
| Float            | FLOAT               |
| Double           | DOUBLE              |
| VarChar          | DECIMAL             |
| String           | STRING              |

## Examples

```hocon
env {
  # You can set engine configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 5000
  #execution.checkpoint.data-uri = "hdfs://localhost:9000/checkpoint"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  LocalFile {
    schema {
      fields {
        bookID = string
        title_1 = string
        title_2 = string
      }
    }
    path = "/tmp/milvus_test/book"
    file_format_type = "csv"
  }
}

transform {
}

sink {
  Milvus {
    milvus_host = localhost
    milvus_port = 19530
    username = root
    password = Milvus
    collection_name = title_db
    openai_engine = text-embedding-ada-002
    openai_api_key = sk-xxxx
    embeddings_fields = title_2
  }
}
```

## Changelog

### next version

- Add Milvus Sink Connector


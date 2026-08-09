import ChangeLog from '../changelog/connector-doris.md';

# Doris

> Doris source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Description

Used to read data from Apache Doris.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Supported DataSource Info

| Datasource |          Supported versions          | Driver | Url | Maven |
|------------|--------------------------------------|--------|-----|-------|
| Doris      | Only Doris2.0 or later is supported. | -      | -   | -     |

## Data Type Mapping

|           Doris Data type            |                                                                 SeaTunnel Data type                                                                 |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| INT                                  | INT                                                                                                                                                 |
| TINYINT                              | TINYINT                                                                                                                                             |
| SMALLINT                             | SMALLINT                                                                                                                                            |
| BIGINT                               | BIGINT                                                                                                                                              |
| LARGEINT                             | STRING                                                                                                                                              |
| BOOLEAN                              | BOOLEAN                                                                                                                                             |
| DECIMAL                              | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| FLOAT                                | FLOAT                                                                                                                                               |
| DOUBLE                               | DOUBLE                                                                                                                                              |
| CHAR<br/>VARCHAR<br/>STRING<br/>TEXT | STRING                                                                                                                                              |
| JSON                                 | STRING                                                                                                                                              |
| VARIANT                              | STRING                                                                                                                                              |
| DATE                                 | DATE                                                                                                                                                |
| DATETIME<br/>DATETIME(p)             | TIMESTAMP                                                                                                                                           |
| ARRAY                                | ARRAY                                                                                                                                               |

## Source Options

Base configuration:

|               Name               |  Type  | Required |  Default   |                                             Description                                             |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| fenodes                          | string | yes      | -          | FE address, the format is `"fe_host:fe_http_port"`                                                  |
| username                         | string | yes      | -          | User username                                                                                       |
| password                         | string | yes      | -          | User password                                                                                       |
| doris.request.retries            | int    | no       | 3          | Number of retries to send requests to Doris FE.                                                     |
| doris.request.read.timeout.ms    | int    | no       | 30000      | Socket read timeout for requests sent to Doris BE.                                                  |
| doris.request.connect.timeout.ms | int    | no       | 30000      | Connection timeout for requests sent to Doris FE or BE.                                             |
| query-port                       | int    | no       | 9030       | Doris query port.                                                                                   |
| doris.request.query.timeout.s    | int    | no       | 3600       | Timeout period of Doris scan data, expressed in seconds.                                            |
| doris.request.tablet.size        | int    | no       | Integer.MAX_VALUE | The number of Doris tablets grouped into each SeaTunnel split. The minimum value is `1`.       |
| doris.deserialize.arrow.async    | boolean | no      | false      | Whether to deserialize Arrow data asynchronously.                                                    |
| doris.request.retriesdoris.deserialize.queue.size | int | no | 64 | Queue size used by asynchronous Arrow deserialization. The key name is the literal runtime option key (it is a known typo in the connector source); use this exact key when tuning the queue size. |
| table_list                       | Array  | no       | -          | List of Doris tables to read.                                                                        |

Table list configuration:

|               Name               |  Type  | Required |  Default   |                                             Description                                             |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| database                         | string | yes      | -          | The name of Doris database                                                                          |
| table                            | string | yes      | -          | The name of Doris table                                                                             |
| doris.read.field                 | string | no       | -          | Use the 'doris.read.field' parameter to select the doris table columns to read                      |
| doris.filter.query               | string | no       | -          | Data filtering in doris. the format is "field = value",example : doris.filter.query = "F_ID > 2"    |
| doris.request.tablet.size        | int    | no       | Integer.MAX_VALUE | The number of Doris tablets grouped into each SeaTunnel split for this table. The minimum value is `1`. |
| doris.batch.size                 | int    | no       | 1024       | The maximum value that can be obtained by reading Doris BE once.                                    |
| doris.exec.mem.limit             | long   | no       | 2147483648 | Maximum memory that can be used by a single be scan request. The default memory is 2G (2147483648). |
 
Note: When this configuration corresponds to a single table, you can flatten the configuration items in table_list to the outer layer. If `table_list` is not configured, `database` and `table` must be configured at the outer source level.

### Tips

> It is not recommended to modify advanced parameters unless you understand the underlying behavior. The values listed under **Default** above are tuned for typical workloads.

## Example

### Single Table

> This example reads a single Doris table and writes the result to Console.

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "doris_e2e:8030"
    username = root
    password = ""
    database = "e2e_source"
    table = "doris_e2e_table"
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

Use the `doris.read.field` parameter to select the Doris table columns to read:

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "doris_e2e:8030"
    username = root
    password = ""
    database = "e2e_source"
    table = "doris_e2e_table"
    doris.read.field = "F_ID,F_INT,F_BIGINT,F_TINYINT,F_SMALLINT"
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

Use `doris.filter.query` to filter the data. The parameter value is passed directly to Doris as a predicate:

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "doris_e2e:8030"
    username = root
    password = ""
    database = "e2e_source"
    table = "doris_e2e_table"
    doris.filter.query = "F_ID > 2"
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

### Multiple Tables

> This example reads from multiple Doris tables and writes them to a Doris sink. The `${table_name}` placeholder is expanded from each `table_list` entry.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "xxxx:8030"
    username = root
    password = ""
    table_list = [
      {
        database = "st_source_0"
        table = "doris_table_0"
        doris.read.field = "F_ID,F_INT,F_BIGINT,F_TINYINT"
        doris.filter.query = "F_ID >= 50"
        doris.request.tablet.size = 1
        doris.exec.mem.limit = 2147483648
      },
      {
        database = "st_source_1"
        table = "doris_table_1"
      }
    ]
  }
}

transform {}

sink {
  Doris {
    fenodes = "xxxx:8030"
    schema_save_mode = "RECREATE_SCHEMA"
    username = root
    password = ""
    database = "st_sink"
    table = "${table_name}"
    sink.enable-2pc = "true"
    sink.label-prefix = "test_json"
    doris.config = {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

## Changelog

<ChangeLog />

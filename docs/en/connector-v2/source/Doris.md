# Doris

> Doris source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Used to read data from Doris.
Doris Source will send a SQL to FE, FE will parse it into an execution plan, send it to BE, and BE will
directly return the data

## Supported DataSource Info

| Datasource |          Supported versions          | Driver | Url | Maven |
|------------|--------------------------------------|--------|-----|-------|
| Doris      | Only Doris2.0 or later is supported. | -      | -   | -     |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/'
> working directory<br/>

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
| DATE                                 | DATE                                                                                                                                                |
| DATETIME<br/>DATETIME(p)             | TIMESTAMP                                                                                                                                           |
| ARRAY                                | ARRAY                                                                                                                                               |

## Source Options

|               Name               |  Type  | Required |  Default   |                                             Description                                             |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| fenodes                          | string | yes      | -          | FE address, the format is `"fe_host:fe_http_port"`                                                  |
| username                         | string | yes      | -          | User username                                                                                       |
| password                         | string | yes      | -          | User password                                                                                       |
| database                         | string | yes      | -          | The name of Doris database                                                                          |
| table                            | string | yes      | -          | The name of Doris table                                                                             |
| doris.read.field                 | string | no       | -          | Use the 'doris.read.field' parameter to select the doris table columns to read                      |
| query-port                       | string | no       | 9030       | Doris QueryPort                                                                                     |
| doris.filter.query               | string | no       | -          | Data filtering in doris. the format is "field = value",example : doris.filter.query = "F_ID > 2"    |
| doris.batch.size                 | int    | no       | 1024       | The maximum value that can be obtained by reading Doris BE once.                                    |
| doris.request.query.timeout.s    | int    | no       | 3600       | Timeout period of Doris scan data, expressed in seconds.                                            |
| doris.exec.mem.limit             | long   | no       | 2147483648 | Maximum memory that can be used by a single be scan request. The default memory is 2G (2147483648). |
| doris.request.retries            | int    | no       | 3          | Number of retries to send requests to Doris FE.                                                     |
| doris.request.read.timeout.ms    | int    | no       | 30000      |                                                                                                     |
| doris.request.connect.timeout.ms | int    | no       | 30000      |                                                                                                     |

### Tips

> It is not recommended to modify advanced parameters at will

## Task Example

> This is an example of reading a Doris table and writing to Console.

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
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
    # please go to https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```

Use the 'doris.read.field' parameter to select the doris table columns to read

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
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
    # please go to https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```

Use 'doris.filter.query' to filter the data, and the parameter values are passed directly to doris

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
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
    # please go to https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```


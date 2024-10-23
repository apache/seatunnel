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
- [x] [support multiple table read](../../concept/connector-v2-features.md)

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

Base configuration:

|               Name               |  Type  | Required |  Default   |                                             Description                                             |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| fenodes                          | string | yes      | -          | FE address, the format is `"fe_host:fe_http_port"`                                                  |
| username                         | string | yes      | -          | User username                                                                                       |
| password                         | string | yes      | -          | User password                                                                                       |
| doris.request.retries            | int    | no       | 3          | Number of retries to send requests to Doris FE.                                                     |
| doris.request.read.timeout.ms    | int    | no       | 30000      |                                                                                                     |
| doris.request.connect.timeout.ms | int    | no       | 30000      |                                                                                                     |
| query-port                       | string | no       | 9030       | Doris QueryPort                                                                                     |
| doris.request.query.timeout.s    | int    | no       | 3600       | Timeout period of Doris scan data, expressed in seconds.                                            |

Table list configuration:

|               Name               |  Type  | Required |  Default   |                                             Description                                             |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| database                         | string | yes      | -          | The name of Doris database                                                                          |
| table                            | string | yes      | -          | The name of Doris table                                                                             |
| doris.read.field                 | string | no       | -          | Use the 'doris.read.field' parameter to select the doris table columns to read                      |
| doris.filter.query               | string | no       | -          | Data filtering in doris. the format is "field = value",example : doris.filter.query = "F_ID > 2"    |
| doris.batch.size                 | int    | no       | 1024       | The maximum value that can be obtained by reading Doris BE once.                                    |
| doris.exec.mem.limit             | long   | no       | 2147483648 | Maximum memory that can be used by a single be scan request. The default memory is 2G (2147483648). |
 
Note: When this configuration corresponds to a single table, you can flatten the configuration items in table_list to the outer layer.

### Tips

> It is not recommended to modify advanced parameters at will

## Example

### single table
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
### Multiple table
```
env{
  parallelism = 1
  job.mode = "BATCH"
}

source{
  Doris {
      fenodes = "xxxx:8030"
      username = root
      password = ""
      table_list = [
          {
            database = "st_source_0"
            table = "doris_table_0"
            doris.read.field = "F_ID,F_INT,F_BIGINT,F_TINYINT,F_SMALLINT,F_DECIMAL,F_LARGEINT,F_BOOLEAN,F_DOUBLE,F_FLOAT,F_CHAR,F_VARCHAR_11,F_STRING,F_DATETIME_P,F_DATETIME,F_DATE,MAP_VARCHAR_BOOLEAN, MAP_CHAR_TINYINT, MAP_STRING_SMALLINT, MAP_INT_INT, MAP_TINYINT_BIGINT, MAP_SMALLINT_LARGEINT, MAP_BIGINT_FLOAT, MAP_LARGEINT_DOUBLE, MAP_STRING_DECIMAL, MAP_DECIMAL_DATE, MAP_DATE_DATETIME, MAP_DATETIME_CHAR, MAP_CHAR_VARCHAR, MAP_VARCHAR_STRING"
            doris.filter.query = "F_ID >= 50"
          },
          {
            database = "st_source_1"
            table = "doris_table_1"
            doris.read.field = "F_ID,F_INT,F_BIGINT,F_TINYINT,F_SMALLINT,F_DECIMAL,F_LARGEINT,F_BOOLEAN,F_DOUBLE,F_FLOAT,F_CHAR,F_VARCHAR_11,F_STRING,F_DATETIME_P,F_DATETIME,F_DATE,MAP_VARCHAR_BOOLEAN, MAP_CHAR_TINYINT, MAP_STRING_SMALLINT, MAP_INT_INT, MAP_TINYINT_BIGINT, MAP_SMALLINT_LARGEINT, MAP_BIGINT_FLOAT, MAP_LARGEINT_DOUBLE, MAP_STRING_DECIMAL, MAP_DECIMAL_DATE, MAP_DATE_DATETIME, MAP_DATETIME_CHAR, MAP_CHAR_VARCHAR, MAP_VARCHAR_STRING"
            doris.filter.query = "F_ID < 40"
          }
      ]
  }
}

transform {}

sink{
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
          format="json"
          read_json_by_line="true"
      }
  }
}
```

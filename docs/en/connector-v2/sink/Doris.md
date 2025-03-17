# Doris

> Doris sink connector

## Support Doris Version

- exactly-once & cdc supported  `Doris version is >= 1.1.x`
- Array data type supported  `Doris version is >= 1.2.x`
- Map data type will be support in `Doris version is 2.x`

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Description

Used to send data to Doris. Both support streaming and batch mode.
The internal implementation of Doris sink connector is cached and imported by stream load in batches.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Sink Options

|              Name              |  Type   | Required |           Default            |                                                                                                                                      Description                                                                                                                                       |
|--------------------------------|---------|----------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fenodes                        | String  | Yes      | -                            | `Doris` cluster fenodes address, the format is `"fe_ip:fe_http_port, ..."`                                                                                                                                                                                                             |
| query-port                     | int     | No       | 9030                         | `Doris` Fenodes query_port                                                                                                                                                                                                                                                             |
| username                       | String  | Yes      | -                            | `Doris` user username                                                                                                                                                                                                                                                                  |
| password                       | String  | Yes      | -                            | `Doris` user password                                                                                                                                                                                                                                                                  |
| database                       | String  | Yes      | -                            | The database name of `Doris` table, use `${database_name}` to represent the upstream table name                                                                                                                                                                                        |
| table                          | String  | Yes      | -                            | The table name of `Doris` table,  use `${table_name}` to represent the upstream table name                                                                                                                                                                                             |
| table.identifier               | String  | Yes      | -                            | The name of `Doris` table, it will deprecate after version 2.3.5, please use `database` and `table` instead.                                                                                                                                                                           |
| sink.label-prefix              | String  | Yes      | -                            | The label prefix used by stream load imports. In the 2pc scenario, global uniqueness is required to ensure the EOS semantics of SeaTunnel.                                                                                                                                             |
| sink.enable-2pc                | bool    | No       | false                        | Whether to enable two-phase commit (2pc), the default is false. For two-phase commit, please refer to [here](https://doris.apache.org/docs/data-operate/transaction?_highlight=two&_highlight=phase#stream-load-2pc).                                                              |
| sink.enable-delete             | bool    | No       | -                            | Whether to enable deletion. This option requires Doris table to enable batch delete function (0.15+ version is enabled by default), and only supports Unique model. you can get more detail at this [link](https://doris.apache.org/docs/dev/data-operate/delete/batch-delete-manual/) |
| sink.check-interval            | int     | No       | 10000                        | check exception with the interval while loading                                                                                                                                                                                                                      |
| sink.max-retries               | int     | No       | 3                            | the max retry times if writing records to database failed                                                                                                                                                                                                            |
| sink.buffer-size               | int     | No       | 256 * 1024                   | the buffer size to cache data for stream load.                                                                                                                                                                                                                       |
| sink.buffer-count              | int     | No       | 3                            | the buffer count to cache data for stream load.                                                                                                                                                                                                                      |
| doris.batch.size               | int     | No       | 1024                         | the batch size of the write to doris each http request, when the row reaches the size or checkpoint is executed, the data of cached will write to server.                                                                                                            |
| needs_unsupported_type_casting | boolean | No       | false                        | Whether to enable the unsupported type casting, such as Decimal64 to Double                                                                                                                                                                                          |
| schema_save_mode               | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | the schema save mode, please refer to `schema_save_mode` below                                                                                                                                                                                                       |
| data_save_mode                 | Enum    | no       | APPEND_DATA                  | the data save mode, please refer to `data_save_mode` below                                                                                                                                                                                                           |
| save_mode_create_template      | string  | no       | see below                    | see below                                                                                                                                                                                                                                                            |
| custom_sql                     | String  | no       | -                            | When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.                                                           |
| doris.config                   | map     | yes      | -                            | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql,and supported formats.                                                                                                                            |

### schema_save_mode[Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode[Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`CUSTOM_PROCESSING`：User defined processing  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

### save_mode_create_template

We use templates to automatically create Doris tables,
which will create corresponding table creation statements based on the type of upstream data and schema type,
and the default template can be modified according to the situation.

Default template:

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
${rowtype_primary_key},
${rowtype_fields}
) ENGINE=OLAP
 UNIQUE KEY (${rowtype_primary_key})
COMMENT '${comment}'
DISTRIBUTED BY HASH (${rowtype_primary_key})
 PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
)
```

If a custom field is filled in the template, such as adding an `id` field

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}`
(   
    id,
    ${rowtype_fields}
) ENGINE = OLAP UNIQUE KEY (${rowtype_primary_key})
    COMMENT '${comment}'
    DISTRIBUTED BY HASH (${rowtype_primary_key})
    PROPERTIES
(
    "replication_num" = "1"
);
```

The connector will automatically obtain the corresponding type from the upstream to complete the filling,
and remove the id field from `rowtype_fields`. This method can be used to customize the modification of field types and attributes.

You can use the following placeholders

- database: Used to get the database in the upstream schema
- table_name: Used to get the table name in the upstream schema
- rowtype_fields: Used to get all the fields in the upstream schema, we will automatically map to the field
  description of Doris
- rowtype_primary_key: Used to get the primary key in the upstream schema (maybe a list)
- rowtype_unique_key: Used to get the unique key in the upstream schema (maybe a list)
- rowtype_duplicate_key: Used to get the duplicate key in the upstream schema (only for doris source, maybe a list)
- comment: Used to get the table comment in the upstream schema

## Data Type Mapping

| Doris Data Type |           SeaTunnel Data Type           |
|-----------------|-----------------------------------------|
| BOOLEAN         | BOOLEAN                                 |
| TINYINT         | TINYINT                                 |
| SMALLINT        | SMALLINT<br/>TINYINT                    |
| INT             | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT          | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT        | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT           | FLOAT                                   |
| DOUBLE          | DOUBLE<br/>FLOAT                        |
| DECIMAL         | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE            | DATE                                    |
| DATETIME        | TIMESTAMP                               |
| CHAR            | STRING                                  |
| VARCHAR         | STRING                                  |
| STRING          | STRING                                  |
| ARRAY           | ARRAY                                   |
| MAP             | MAP                                     |
| JSON            | STRING                                  |
| HLL             | Not supported yet                       |
| BITMAP          | Not supported yet                       |
| QUANTILE_STATE  | Not supported yet                       |
| STRUCT          | Not supported yet                       |

#### Supported import data formats

The supported formats include CSV and JSON

## Tuning Guide
Appropriately increasing the value of `sink.buffer-size` and `doris.batch.size` can increase the write performance.

In stream mode, if the `doris.batch.size` and `checkpoint.interval` are both configured with a large value, The last data to arrive may have a large delay(The delay time is the checkpoint interval).

This is because the total amount of data arriving at the end may not exceed the threshold specified by `doris.batch.size`. Therefore, commit can only be triggered by checkpoint before the volume of received data does not exceed this threshold. Therefore, you should select an appropriate `checkpoint.interval`.

Otherwise, if you enable the 2pc by the property `sink.enable-2pc=true`.The `sink.buffer-size` will have no effect. So only the checkpoint can trigger the commit.

## Task Example

### Simple:

> The following example describes writing multiple data types to Doris, and users need to create corresponding tables downstream

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    }
}

sink {
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

### CDC(Change Data Capture) Event:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to Doris Sink,FakeSource simulates CDC data with schema, score (int type),Doris needs to create a table sink named test.e2e_table_sink and a corresponding table for it.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
        sex = boolean
        number = tinyint
        height = float
        sight = double
        create_time = date
        update_time = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "A", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = INSERT
        fields = [2, "B", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = INSERT
        fields = [3, "C", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = UPDATE_BEFORE
        fields = [1, "A", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = UPDATE_AFTER
        fields = [1, "A_1", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = DELETE
        fields = [2, "B", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      }
    ]
  }
}

sink {
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}

```

### Use JSON format to import data

```
sink {
    Doris {
        fenodes = "e2e_dorisdb:8030"
        username = root
        password = ""
        database = "test"
        table = "e2e_table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_json"
        doris.config = {
            format="json"
            read_json_by_line="true"
        }
    }
}

```

### Use CSV format to import data

```
sink {
    Doris {
        fenodes = "e2e_dorisdb:8030"
        username = root
        password = ""
        database = "test"
        table = "e2e_table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_csv"
        doris.config = {
          format = "csv"
          column_separator = ","
        }
    }
}
```

### Multiple table

#### example1

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    database = "${database_name}_test"
    table = "${table_name}_test"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

#### example2

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    user = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    database = "${schema_name}_test"
    table = "${table_name}_test"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

## Changelog

### 2.3.0-beta 2022-10-20

- Add Doris Sink Connector

### Next version

- [Improve] Change Doris Config Prefix [3856](https://github.com/apache/seatunnel/pull/3856)

- [Improve] Refactor some Doris Sink code as well as support 2pc and cdc [4235](https://github.com/apache/seatunnel/pull/4235)

:::tip

PR 4235 is an incompatible modification to PR 3856. Please refer to PR 4235 to use the new Doris connector

:::

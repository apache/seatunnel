# PostgreSql

> JDBC PostgreSql Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Supported DataSource Info

| Datasource |                     Supported Versions                     |        Driver         |                  Url                  |                                  Maven                                   |
|------------|------------------------------------------------------------|-----------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL | Different dependency version has different driver class.   | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| PostgreSQL | If you want to manipulate the GEOMETRY type in PostgreSQL. | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)  |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example PostgreSQL datasource: cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/<br/>
> If you want to manipulate the GEOMETRY type in PostgreSQL, add postgresql-xxx.jar and postgis-jdbc-xxx.jar to $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                                       PostgreSQL Data Type                                       |                                                              SeaTunnel Data Type                                                               |
|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                                                        | BOOLEAN                                                                                                                                        |
| _BOOL<br/>                                                                                       | ARRAY&LT;BOOLEAN&GT;                                                                                                                           |
| BYTEA<br/>                                                                                       | BYTES                                                                                                                                          |
| _BYTEA<br/>                                                                                      | ARRAY&LT;TINYINT&GT;                                                                                                                           |
| INT2<br/>SMALLSERIAL<br/>INT4<br/>SERIAL<br/>                                                    | INT                                                                                                                                            |
| _INT2<br/>_INT4<br/>                                                                             | ARRAY&LT;INT&GT;                                                                                                                               |
| INT8<br/>BIGSERIAL<br/>                                                                          | BIGINT                                                                                                                                         |
| _INT8<br/>                                                                                       | ARRAY&LT;BIGINT&GT;                                                                                                                            |
| FLOAT4<br/>                                                                                      | FLOAT                                                                                                                                          |
| _FLOAT4<br/>                                                                                     | ARRAY&LT;FLOAT&GT;                                                                                                                             |
| FLOAT8<br/>                                                                                      | DOUBLE                                                                                                                                         |
| _FLOAT8<br/>                                                                                     | ARRAY&LT;DOUBLE&GT;                                                                                                                            |
| NUMERIC(Get the designated column's specified column size>0)                                     | DECIMAL(Get the designated column's specified column size,Gets the number of digits in the specified column to the right of the decimal point) |
| NUMERIC(Get the designated column's specified column size<0)                                     | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB<br/>UUID | STRING                                                                                                                                         |
| _BPCHAR<br/>_CHARACTER<br/>_VARCHAR<br/>_TEXT                                                    | ARRAY&LT;STRING&GT;                                                                                                                            |
| TIMESTAMP<br/>                                                                                   | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                                                        | TIME                                                                                                                                           |
| DATE<br/>                                                                                        | DATE                                                                                                                                           |
| OTHER DATA TYPES                                                                                 | NOT SUPPORTED YET                                                                                                                              |

## Options

|                   Name                    |  Type   | Required |           Default            |                                                                                                                                                                                                                                                                                    Description                                                                                                                                                                                                                                                                                    |
|-------------------------------------------|---------|----------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -                            | The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost:5432/test <br/>  if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option                                                                                                                                                                                                                                                                                                                                                                                        |
| driver                                    | String  | Yes      | -                            | The jdbc class name used to connect to the remote data source,<br/> if you use PostgreSQL the value is `org.postgresql.Driver`.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| user                                      | String  | No       | -                            | Connection instance user name                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| password                                  | String  | No       | -                            | Connection instance password                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| query                                     | String  | No       | -                            | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| database                                  | String  | No       | -                            | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                                                                                                                                                                                                                                                                                                                                                          |
| table                                     | String  | No       | -                            | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.The table parameter can fill in the name of an unwilling table, which will eventually be used as the table name of the creation table, and supports variables (`${table_name}`, `${schema_name}`). Replacement rules: `${schema_name}` will replace the SCHEMA name passed to the target side, and `${table_name}` will replace the name of the table passed to the table at the target side. |
| primary_keys                              | Array   | No       | -                            | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false                        | Choose to use INSERT sql, UPDATE sql to process update events(INSERT, UPDATE_AFTER) based on query primary key exists. This configuration is only used when database unsupport upsert syntax. **Note**: that this method has low performance                                                                                                                                                                                                                                                                                                                                      |
| connection_check_timeout_sec              | Int     | No       | 30                           | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| max_retries                               | Int     | No       | 0                            | The number of retries to submit failed (executeBatch)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| batch_size                                | Int     | No       | 1000                         | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`<br/>, the data will be flushed into the database                                                                                                                                                                                                                                                                                                                                                                                              |
| is_exactly_once                           | Boolean | No       | false                        | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`.                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| generate_sink_sql                         | Boolean | No       | false                        | Generate sql statements based on the database table you want to write to.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| xa_data_source_class_name                 | String  | No       | -                            | The xa data source class name of the database Driver, for example, PostgreSQL is `org.postgresql.xa.PGXADataSource`, and<br/>please refer to appendix for other data sources                                                                                                                                                                                                                                                                                                                                                                                                      |
| max_commit_attempts                       | Int     | No       | 3                            | The number of retries for transaction commit failures                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| transaction_timeout_sec                   | Int     | No       | -1                           | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                                                                                                                                                                                                                                                                                                                                                               |
| auto_commit                               | Boolean | No       | true                         | Automatic transaction commit is enabled by default                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| field_ide                                 | String  | No       | -                            | Identify whether the field needs to be converted when synchronizing from the source to the sink. `ORIGINAL` indicates no conversion is needed;`UPPERCASE` indicates conversion to uppercase;`LOWERCASE` indicates conversion to lowercase.                                                                                                                                                                                                                                                                                                                                        |
| properties                                | Map     | No       | -                            | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL.                                                                                                                                                                                                                                                                                                                                    |
| common-options                            |         | no       | -                            | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| schema_save_mode                          | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| data_save_mode                            | Enum    | no       | APPEND_DATA                  | Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| custom_sql                                | String  | no       | -                            | When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.                                                                                                                                                                                                                                                                                                                                                                        |
| enable_upsert                             | Boolean | No       | true                         | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### table [string]

Use `database` and this `table-name` auto-generate sql and receive upstream input datas write to database.

This option is mutually exclusive with `query` and has a higher priority.

The table parameter can fill in the name of an unwilling table, which will eventually be used as the table name of the creation table, and supports variables (`${table_name}`, `${schema_name}`). Replacement rules: `${schema_name}` will replace the SCHEMA name passed to the target side, and `${table_name}` will replace the name of the table passed to the table at the target side.

for example:
1. ${schema_name}.${table_name} _test
2. dbo.tt_${table_name} _sink
3. public.sink_table

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

### custom_sql[String]

When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your PostgreSQL. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2
}

sink {
    jdbc {
       # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = root
        password = 123456
        query = "insert into test_table(name,age) values(?,?)"
     }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### Generate Sink SQL

> This example  not need to write complex sql statements, you can configure the database name table name to automatically generate add statements for you

```
sink {
    Jdbc {
        # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = org.postgresql.Driver
        user = root
        password = 123456
        
        generate_sink_sql = true
        database = test
        table = "public.test_table"
    }
}
```

### Exactly-once :

> For accurate write scene we guarantee accurate once

```
sink {
    jdbc {
       # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
    
        max_retries = 0
        user = root
        password = 123456
        query = "insert into test_table(name,age) values(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "org.postgresql.xa.PGXADataSource"
    }
}
```

### CDC(Change Data Capture) Event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
sink {
    jdbc {
        # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = root
        password = 123456
        
        generate_sink_sql = true
        # You need to configure both database and table
        database = test
        table = sink_table
        primary_keys = ["id","name"]
        field_ide = UPPERCASE
    }
}
```

### Save mode function

```
sink {
    Jdbc {
        # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = org.postgresql.Driver
        user = root
        password = 123456
        
        generate_sink_sql = true
        database = test
        table = "public.test_table"
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode="APPEND_DATA"
    }
}
```


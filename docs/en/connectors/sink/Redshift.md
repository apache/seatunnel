import ChangeLog from '../changelog/connector-jdbc.md';

# Redshift

> JDBC Redshift Sink Connector

## Support Redshift Version

- Amazon Redshift

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data through JDBC. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource |                    Supported Versions                    |             Driver              |                   Url                   |                                       Maven                                        |
|------------|----------------------------------------------------------|---------------------------------|-----------------------------------------|------------------------------------------------------------------------------------|
| Redshift   | Different dependency version has different driver class. | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [Download](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## Data Type Mapping

|      SeaTunnel Data Type       |  Redshift Data Type  |
|--------------------------------|----------------------|
| BOOLEAN                        | BOOLEAN              |
| TINYINT<br/>SMALLINT           | SMALLINT             |
| INT                            | INTEGER              |
| BIGINT                         | BIGINT               |
| FLOAT                          | REAL                 |
| DOUBLE                         | DOUBLE PRECISION     |
| DECIMAL                        | NUMERIC              |
| STRING(<=65535)                | CHARACTER VARYING    |
| STRING(>65535)                 | SUPER                |
| BYTES                          | BINARY VARYING       |
| TIME                           | TIME                 |
| TIMESTAMP                      | TIMESTAMP            |
| MAP<br/>ARRAY<br/>ROW          | SUPER                |

## Sink Options

> Redshift sink is implemented on top of the JDBC sink. The table below focuses on the most commonly used Redshift options. For inherited advanced JDBC sink options such as `compatible_mode`, `dialect`, `is_primary_key_updated`, `support_upsert_by_insert_only`, `use_copy_statement`, `tablePrefix`, `tableSuffix`, and `create_index`, see [JDBC Sink](Jdbc.md).

|                   Name                    |  Type   | Required |           Default            |                                                                                                                  Description                                                                                                                   |
|-------------------------------------------|---------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -                            | The URL of the JDBC connection. Refer to a case: `jdbc:redshift://localhost:5439/mydatabase`                                                                                                                                                   |
| driver                                    | String  | Yes      | -                            | The jdbc class name used to connect to the remote data source, the value is `com.amazon.redshift.jdbc.Driver`.                                                                                                                                 |
| username                                  | String  | No       | -                            | Connection instance user name                                                                                                                                                                                                                  |
| password                                  | String  | No       | -                            | Connection instance password                                                                                                                                                                                                                   |
| query                                     | String  | No       | -                            | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                         |
| database                                  | String  | No       | -                            | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                        |
| table                                     | String  | No       | -                            | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                            |
| schema                                    | String  | No       | -                            | The schema name of the target table in Redshift. SeaTunnel does not apply a default value for this option. Configure it explicitly when the target table is not already schema-qualified or when using catalog-based operations such as `generate_sink_sql`, `schema_save_mode`, and `data_save_mode`. |
| primary_keys                              | Array   | No       | -                            | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                            |
| connection_check_timeout_sec              | Int     | No       | 30                           | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                             |
| max_retries                               | Int     | No       | 0                            | The number of retries to submit failed (executeBatch)                                                                                                                                                                                          |
| batch_size                                | Int     | No       | 1000                         | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`<br/>, the data will be flushed into the database                                                           |
| is_exactly_once                           | Boolean | No       | false                        | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`.                                                                                                              |
| generate_sink_sql                         | Boolean | No       | false                        | Generate sql statements based on the database table you want to write to                                                                                                                                                                       |
| xa_data_source_class_name                 | String  | No       | -                            | The xa data source class name of the database Driver, for Redshift it is `com.amazon.redshift.xa.RedshiftXADataSource`                                                                                                                        |
| max_commit_attempts                       | Int     | No       | 3                            | The number of retries for transaction commit failures                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1                           | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                            |
| auto_commit                               | Boolean | No       | true                         | Automatic transaction commit is enabled by default                                                                                                                                                                                             |
| field_ide                                 | String  | No       | -                            | Identify whether the field needs to be converted when synchronizing from the source to the sink. `ORIGINAL` indicates no conversion is needed;`UPPERCASE` indicates conversion to uppercase;`LOWERCASE` indicates conversion to lowercase.      |
| properties                                | Map     | No       | -                            | Additional connection configuration parameters, when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in Redshift, properties take precedence over the URL. |
| common-options                            |         | No       | -                            | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details                                                                                                                     |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.                                                                                                       |
| data_save_mode                            | Enum    | No       | APPEND_DATA                  | Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.                                                                                                                  |
| custom_sql                                | String  | No       | -                            | When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.                                      |
| enable_upsert                             | Boolean | No       | true                         | Enable upsert by primary_keys exist, If the task only has `insert`, setting this parameter to `false` can speed up data import                                                                                                                 |
| multi_table_sink_replica                  | Int     | No       | 1                            | The number of replicas for multi-table write, when `multi_table_sink_replica > 1`, the data will be written to multiple tables in parallel                                                                                                     |

## Task Example

### Simple

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database and table test_table in your Redshift. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../getting-started/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../getting-started/locally/quick-start-seatunnel-engine.md) to run this job.

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
}

transform {
}

sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "myUser"
        password = "myPassword"
        query = "insert into test_table(name,age) values(?,?)"
    }
}
```

### Generate Sink SQL

> This example does not need to write complex sql statements, you can configure the database name and table name to automatically generate add statements for you

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "myUser"
        password = "myPassword"
        generate_sink_sql = true
        database = mydatabase
        schema = "public"
        table = test_table
    }
}
```

### Exactly-Once

> For accurate write scene we guarantee accurate once

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        max_retries = 0
        username = "myUser"
        password = "myPassword"
        query = "insert into test_table(name,age) values(?,?)"
        is_exactly_once = "true"
        xa_data_source_class_name = "com.amazon.redshift.xa.RedshiftXADataSource"
    }
}
```

### CDC(Change Data Capture) Event

> CDC change data is also supported by us. In this case, you need config database, table and primary_keys.

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "myUser"
        password = "myPassword"
        generate_sink_sql = true
        database = mydatabase
        schema = "public"
        table = sink_table
        primary_keys = ["id","name"]
        field_ide = UPPERCASE
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

### Multiple Table Sync

#### Example 1: CDC Multiple Table Sync to Redshift

> Sync multiple tables from a CDC source to target Redshift database, using placeholders for dynamic table name mapping

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role","seatunnel.user","seatunnel.order"]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:redshift://redshift-cluster.xxxx.region.redshift.amazonaws.com:5439/mydatabase"
    driver = "com.amazon.redshift.jdbc.Driver"
    username = "myUser"
    password = "myPassword"
    generate_sink_sql = true
    database = mydatabase
    schema = "public"
    table = "${table_name}_sink"
    primary_keys = ["${primary_key}"]
  }
}
```

#### Example 2: JDBC Source Multiple Table Sync to Redshift

> Batch sync multiple tables from a database using JDBC Source to Redshift

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = com.mysql.cj.jdbc.Driver
    url = "jdbc:mysql://localhost:3306/source_db"
    username = "root"
    password = "123456"
    table_list = [
      {
        table_path = "source_db.table_1"
      },
      {
        table_path = "source_db.table_2"
      }
    ]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:redshift://redshift-cluster.xxxx.region.redshift.amazonaws.com:5439/mydatabase"
    driver = "com.amazon.redshift.jdbc.Driver"
    username = "myUser"
    password = "myPassword"
    generate_sink_sql = true
    database = mydatabase
    schema = "public"
    table = "${table_name}_copy"
    primary_keys = ["${primary_key}"]
  }
}
```

## Changelog

<ChangeLog />

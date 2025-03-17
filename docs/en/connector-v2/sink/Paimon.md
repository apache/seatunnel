# Paimon

> Paimon sink connector

## Description

Sink connector for Apache Paimon. It can support cdc mode 、auto create table.

## Supported DataSource Info

| Datasource | Dependent |                                   Maven                                   |
|------------|-----------|---------------------------------------------------------------------------|
| Paimon     | hive-exec | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)  |
| Paimon     | libfb303  | [Download](https://mvnrepository.com/artifact/org.apache.thrift/libfb303) |

## Database Dependency

> In order to be compatible with different versions of Hadoop and Hive, the scope of hive-exec in the project pom file are provided, so if you use the Flink engine, first you may need to add the following Jar packages to <FLINK_HOME>/lib directory, if you are using the Spark engine and integrated with Hadoop, then you do not need to add the following Jar packages.

```
hive-exec-xxx.jar
libfb303-xxx.jar
```

> Some versions of the hive-exec package do not have libfb303-xxx.jar, so you also need to manually import the Jar package.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Options

|            name             | type   | required | default value                | Description                                                                                                                                                      |
|-----------------------------|--------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| warehouse                   | String | Yes      | -                            | Paimon warehouse path                                                                                                                                            |
| catalog_type                | String | No       | filesystem                   | Catalog type of Paimon, support filesystem and hive                                                                                                              |
| catalog_uri                 | String | No       | -                            | Catalog uri of Paimon, only needed when catalog_type is hive                                                                                                     |
| database                    | String | Yes      | -                            | The database you want to access                                                                                                                                  |
| table                       | String | Yes      | -                            | The table you want to access                                                                                                                                     |
| hdfs_site_path              | String | No       | -                            | The path of hdfs-site.xml                                                                                                                                        |
| schema_save_mode            | Enum   | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | The schema save mode                                                                                                                                             |
| data_save_mode              | Enum   | No       | APPEND_DATA                  | The data save mode                                                                                                                                               |
| paimon.table.primary-keys   | String | No       | -                            | Default comma-separated list of columns (primary key) that identify a row in tables.(Notice: The partition field needs to be included in the primary key fields) |
| paimon.table.partition-keys | String | No       | -                            | Default comma-separated list of partition fields to use when creating tables.                                                                                    |
| paimon.table.write-props    | Map    | No       | -                            | Properties passed through to paimon table initialization, [reference](https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions).            |
| paimon.hadoop.conf          | Map    | No       | -                            | Properties in hadoop conf                                                                                                                                        |
| paimon.hadoop.conf-path     | String | No       | -                            | The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files                                                                       |

## Checkpoint in batch mode

When you set `checkpoint.interval` to a value greater than 0 in batch mode, the paimon connector will commit the data to the paimon table when the checkpoint triggers after a certain number of records have been written. At this moment, the written data in paimon that is visible. 
However, if you do not set `checkpoint.interval` in batch mode, the paimon sink connector will commit the data after all records are written. The written data in paimon that is not visible until the batch task completes.

## Changelog
You must configure the `changelog-producer=input` option to enable the changelog producer mode of the paimon table. If you use the auto-create table function of paimon sink, you can configure this property in `paimon.table.write-props`.

The changelog producer mode of the paimon table has [four mode](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/) which is `none`、`input`、`lookup` and `full-compaction`.

All `changelog-producer` modes are currently supported. The default is `none`.

* [`none`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#none)
* [`input`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#input)
* [`lookup`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#lookup)
* [`full-compaction`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#full-compaction)
> note： 
> When you use a streaming mode to read paimon table，different mode will produce [different results](https://github.com/apache/seatunnel/blob/dev/docs/en/connector-v2/source/Paimon.md#changelog)。

## Filesystems
The Paimon connector supports writing data to multiple file systems. Currently, the supported file systems are hdfs and s3.
If you use the s3 filesystem. You can configure the `fs.s3a.access-key`、`fs.s3a.secret-key`、`fs.s3a.endpoint`、`fs.s3a.path.style.access`、`fs.s3a.aws.credentials.provider` properties in the `paimon.hadoop.conf` option.
Besides, the warehouse should start with `s3a://`.

## Schema Evolution
Cdc Ingestion supports a limited number of schema changes. Currently supported schema changes includes:

* Adding columns.

* Modify column. More specifically, If you modify the column type, the following changes are supported:

    * altering from a string type (char, varchar, text) to another string type with longer length,
    * altering from a binary type (binary, varbinary, blob) to another binary type with longer length,
    * altering from an integer type (tinyint, smallint, int, bigint) to another integer type with wider range,
    * altering from a floating-point type (float, double) to another floating-point type with wider range,
  
  are supported. 
  > Note:
  > 
  > If {oldType} and {newType} belongs to the same type family, but old type has higher precision than new type. Ignore this convert.

* Drop columns.

* Change columns.


## Examples
### Schema evolution
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
    schema-changes.enabled = true
  }
}

sink {
  Paimon {
    warehouse = "file:///tmp/paimon"
    database = "mysql_to_paimon"
    table = "products"
  }
}
```

### Single table

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
    table-names = ["seatunnel.role"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
  }
}
```

### Single table with s3 filesystem

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
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
      }
    }
  }
}

sink {
  Paimon {
    warehouse = "s3a://test/"
    database = "seatunnel_namespace11"
    table = "st_test"
    paimon.hadoop.conf = {
        fs.s3a.access-key=G52pnxg67819khOZ9ezX
        fs.s3a.secret-key=SHJuAQqHsLrgZWikvMa3lJf5T0NfM5LMFliJh9HF
        fs.s3a.endpoint="http://minio4:9000"
        fs.s3a.path.style.access=true
        fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
    }
  }
}
```

### Single table(Specify hadoop HA config and kerberos config)

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
    table-names = ["seatunnel.role"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="hdfs:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
    paimon.hadoop.conf = {
      fs.defaultFS = "hdfs://nameservice1"
      dfs.nameservices = "nameservice1"
      dfs.ha.namenodes.nameservice1 = "nn1,nn2"
      dfs.namenode.rpc-address.nameservice1.nn1 = "hadoop03:8020"
      dfs.namenode.rpc-address.nameservice1.nn2 = "hadoop04:8020"
      dfs.client.failover.proxy.provider.nameservice1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      dfs.client.use.datanode.hostname = "true"
      security.kerberos.login.principal = "your-kerberos-principal"
      security.kerberos.login.keytab = "your-kerberos-keytab-path"
    }
  }
}
```

### Single table(Specify hadoop HA config with hadoop_user_name) 

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
    table-names = ["seatunnel.role"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="hdfs:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
    paimon.hadoop.conf = {
      hadoop_user_name = "hdfs"
      fs.defaultFS = "hdfs://nameservice1"
      dfs.nameservices = "nameservice1"
      dfs.ha.namenodes.nameservice1 = "nn1,nn2"
      dfs.namenode.rpc-address.nameservice1.nn1 = "hadoop03:8020"
      dfs.namenode.rpc-address.nameservice1.nn2 = "hadoop04:8020"
      dfs.client.failover.proxy.provider.nameservice1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      dfs.client.use.datanode.hostname = "true"
      security.kerberos.login.principal = "your-kerberos-principal"
      security.kerberos.login.keytab = "your-kerberos-keytab-path"
    }
  }
}
```

### Single table(Hive catalog)

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
      }
      primaryKey {
        name = "pk_id"
        columnNames = [pk_id]
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "A", 100]
      },
      {
        kind = INSERT
        fields = [2, "B", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      }
      {
        kind = UPDATE_BEFORE
        fields = [1, "A", 100]
      },
      {
        kind = UPDATE_AFTER
        fields = [1, "A_1", 100]
      },
      {
        kind = DELETE
        fields = [2, "B", 100]
      }
    ]
  }
}

sink {
  Paimon {
    schema_save_mode = "RECREATE_SCHEMA"
    catalog_name="seatunnel_test"
    catalog_type="hive"
    catalog_uri="thrift://hadoop04:9083"
    warehouse="hdfs:///tmp/seatunnel"
    database="seatunnel_test"
    table="st_test3"
    paimon.hadoop.conf = {
      fs.defaultFS = "hdfs://nameservice1"
      dfs.nameservices = "nameservice1"
      dfs.ha.namenodes.nameservice1 = "nn1,nn2"
      dfs.namenode.rpc-address.nameservice1.nn1 = "hadoop03:8020"
      dfs.namenode.rpc-address.nameservice1.nn2 = "hadoop04:8020"
      dfs.client.failover.proxy.provider.nameservice1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      dfs.client.use.datanode.hostname = "true"
    }
  }
}

```

### Single table with write props of paimon

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
    table-names = ["seatunnel.role"]
  }
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
    paimon.table.write-props = {
        bucket = 2
        file.format = "parquet"
    }
    paimon.table.partition-keys = "dt"
    paimon.table.primary-keys = "pk_id,dt"
  }
}
```

#### Write with the `changelog-producer` attribute

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
  table-names = ["seatunnel.role"]
 }
}

sink {
 Paimon {
  catalog_name = "seatunnel_test"
  warehouse = "file:///tmp/seatunnel/paimon/hadoop-sink/"
  database = "seatunnel"
  table = "role"
  paimon.table.write-props = {
   changelog-producer = full-compaction
   changelog-tmp-path = /tmp/paimon/changelog
  }
 }
}
```

### Write to dynamic bucket table 

Single dynamic bucket table with write props of paimon，operates on the primary key table and bucket is -1.

#### core options

Please [reference](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/#dynamic-bucket)

|              name              | type | required | default values |                  Description                   |
|--------------------------------|------|----------|----------------|------------------------------------------------|
| dynamic-bucket.target-row-num  | long | yes      | 2000000L       | controls the target row number for one bucket. |
| dynamic-bucket.initial-buckets | int  | no       |                | controls the number of initialized bucket.     |

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
    table-names = ["seatunnel.role"]
  }
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
    paimon.table.write-props = {
        bucket = -1
        dynamic-bucket.target-row-num = 50000
    }
    paimon.table.partition-keys = "dt"
    paimon.table.primary-keys = "pk_id,dt"
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
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="${database_name}_test"
    table="${table_name}_test"
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
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="${schema_name}_test"
    table="${table_name}_test"
  }
}
```


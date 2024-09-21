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

|            name             |  type  | required |        default value         | Description                                                                                                                                                      |
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
| paimon.table.write-props    | Map    | No       | -                            | Properties passed through to paimon table initialization, [reference](https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions).               |
| paimon.hadoop.conf          | Map    | No       | -                            | Properties in hadoop conf                                                                                                                                        |
| paimon.hadoop.conf-path     | String | No       | -                            | The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files                                                                       |

## Examples

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


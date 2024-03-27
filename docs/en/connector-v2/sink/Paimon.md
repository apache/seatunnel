# Paimon

> Paimon sink connector

## Description

Sink connector for Apache Paimon. It can support cdc mode 、auto create table.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

## Options

|            name             |  type  | required |        default value         |                                                                           Description                                                                            |
|-----------------------------|--------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| warehouse                   | String | Yes      | -                            | Paimon warehouse path                                                                                                                                            |
| database                    | String | Yes      | -                            | The database you want to access                                                                                                                                  |
| table                       | String | Yes      | -                            | The table you want to access                                                                                                                                     |
| hdfs_site_path              | String | No       | -                            | The path of hdfs-site.xml                                                                                                                                        |
| schema_save_mode            | Enum   | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | The schema save mode                                                                                                                                             |
| data_save_mode              | Enum   | No       | APPEND_DATA                  | The data save mode                                                                                                                                               |
| paimon.table.primary-keys   | String | No       | -                            | Default comma-separated list of columns (primary key) that identify a row in tables.(Notice: The partition field needs to be included in the primary key fields) |
| paimon.table.partition-keys | String | No       | -                            | Default comma-separated list of partition fields to use when creating tables.                                                                                    |
| paimon.table.write-props    | Map    | No       | -                            | Properties passed through to paimon table initialization, [reference](https://paimon.apache.org/docs/0.6/maintenance/configurations/#coreoptions).               |
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

### Multiple table

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
    database="${database_name}"
    table="${table_name}"
  }
}
```


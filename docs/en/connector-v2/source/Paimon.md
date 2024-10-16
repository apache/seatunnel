# Paimon

> Paimon source connector

## Description

Read data from Apache Paimon.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|          name           |  type  | required | default value |
|-------------------------|--------|----------|---------------|
| warehouse               | String | Yes      | -             |
| catalog_type            | String | No       | filesystem    |
| catalog_uri             | String | No       | -             |
| database                | String | Yes      | -             |
| table                   | String | Yes      | -             |
| hdfs_site_path          | String | No       | -             |
| query                   | String | No       | -             |
| paimon.hadoop.conf      | Map    | No       | -             |
| paimon.hadoop.conf-path | String | No       | -             |

### warehouse [string]

Paimon warehouse path

### catalog_type [string]

Catalog type of Paimon, support filesystem and hive

### catalog_uri [string]

Catalog uri of Paimon, only needed when catalog_type is hive

### database [string]

The database you want to access

### table [string]

The table you want to access

### hdfs_site_path [string]

The file path of `hdfs-site.xml`

### query [string]

The filter condition of the table read. For example: `select * from st_test where id > 100`. If not specified, all rows are read.
Currently, where conditions only support <, <=, >, >=, =, !=, or, and,is null, is not null, and others are not supported.
The Having, Group By, Order By clauses are currently unsupported, because these clauses are not supported by Paimon.
The projection and limit will be supported in the future.

Note: When the field after the where condition is a string or boolean value, its value must be enclosed in single quotes, otherwise an error will be reported. `For example: name='abc' or tag='true'`
The field data types currently supported by where conditions are as follows:

* string
* boolean
* tinyint
* smallint
* int
* bigint
* float
* double
* date
* timestamp

### paimon.hadoop.conf [string]

Properties in hadoop conf

### paimon.hadoop.conf-path [string]

The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files

## Examples

### Simple example

```hocon
source {
 Paimon {
     warehouse = "/tmp/paimon"
     database = "default"
     table = "st_test"
   }
}
```

### Filter example

```hocon
source {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "full_type"
    table = "st_test"
    query = "select c_boolean, c_tinyint from st_test where c_boolean= 'true' and c_tinyint > 116 and c_smallint = 15987 or c_decimal='2924137191386439303744.39292213'"
  }
}
```

### Hadoop conf example

```hocon
source {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="hdfs:///tmp/paimon"
    database="seatunnel_namespace1"
    table="st_test"
    query = "select * from st_test where pk_id is not null and pk_id < 3"
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

### Hive catalog example

```hocon
source {
  Paimon {
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

## Changelog
If you want to read the changelog of this connector, your sink table of paimon which mast has the options named `changelog-producer=input`, then you can refer to [Paimon changelog](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/).
Currently, we only support the `input` and `none` mode of changelog producer. If the changelog producer is `input`, the streaming read of the connector will generate -U,+U,+I,+D data. But if the changelog producer is `none`, the streaming read of the connector will generate +I,+U,+D data.

### Streaming read example
```hocon
env {
  parallelism = 1
  job.mode = "Streaming"
}

source {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "full_type"
    table = "st_test"
  }
}

sink {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "full_type"
    table = "st_test_sink"
    paimon.table.primary-keys = "c_tinyint"
  }
}
```

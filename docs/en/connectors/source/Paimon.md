import ChangeLog from '../changelog/connector-paimon.md';

# Paimon

> Paimon source connector

## Description

Read data from Apache Paimon.

### Comparison between SeaTunnel and Paimon version

| Seatunnel Version | Paimon Version   |
|-------------------|------------------|
| 2.3.2  -  2.3.3   | 0.4-SNAPSHOT     |
| 2.3.4             | 0.6-SNAPSHOT     |
| 2.3.5  -  2.3.11  | 0.7.0-incubating |
| 2.3.12  - 2.3.13  | 1.1.1            |

### Key Considerations for Upgrading Paimon from `0.7.0-incubating` to `1.1.1`

1. **Backup Recommendations**
   Although compatibility is ensured, it is strongly recommended to backup critical data, especially the metadata directory, before initiating the upgrade.
2. **Gradual Upgrade Process**
    - **Test Environment Validation**: First validate the upgrade process in a staging environment.
    - **Update JAR Files**: Replace Paimon JAR files with version 1.1.1.
    - **Automatic Format Upgrade**: The system will automatically detect and upgrade older file formats.
3. **Configuration Check**
   Review your configurations to ensure no deprecated options are in use. While most configurations remain backward-compatible, deprecated settings may require updates.
4. **Post-Upgrade Validation**
   Verify the following after upgrading:
    - **Read/Write Operations**: Ensure data ingestion and retrieval workflows function normally.
    - **Query Performance**: Confirm that query response times meet expectations.
    - **New Feature Verification**: Test all newly introduced features (e.g., time travel, enhanced compaction) to ensure proper functionality.

**Note**: These steps help minimize risks and ensure a smooth transition to the stable version 1.1.1.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name                    | type     | required       | default value | description                                                                                                                  |
|-------------------------|----------|----------------|---------------|------------------------------------------------------------------------------------------------------------------------------|
| warehouse               | String   | Yes            | -             | Paimon warehouse path.                                                                                                       |
| catalog_name            | String   | No             | paimon        | The name of the Paimon catalog.                                                                                              |
| catalog_type            | String   | No             | filesystem    | Catalog type of Paimon, supports `filesystem` and `hive`.                                                                    |
| catalog_uri             | String   | Yes when `catalog_type` is `hive` | -             | Catalog URI of Paimon. Required when `catalog_type` is `hive`.                                                                |
| database                | String   | Yes            | -             | The database you want to access.                                                                                             |
| table                   | String   | Yes when `table_list` is absent | -             | The table you want to access. Configure exactly one of `table` and `table_list`.                                              |
| table_list              | array    | Yes when `table` is absent | -             | The list of tables to read. Each item must contain `table`, and may contain its own `query`.                                  |
| user                    | String   | No             | -             | The Paimon user used to access the table (for example, with a `hive` catalog that enables authentication).                   |
| password                | String   | No             | -             | The Paimon user password. Required when `user` is configured.                                                                 |
| hdfs_site_path          | String   | No             | -             | The file path of `hdfs-site.xml`. Deprecated; prefer `paimon.hadoop.conf` or `paimon.hadoop.conf-path` for new jobs.        |
| query                   | String   | No             | -             | The filter condition applied to the table read. If not specified, all rows are read.                                         |
| paimon.hadoop.conf      | Map      | No             | -             | Properties applied to the Hadoop configuration.                                                                              |
| paimon.hadoop.conf-path | String   | No             | -             | The loading path for `core-site.xml`, `hdfs-site.xml`, and `hive-site.xml` files.                                            |

### warehouse [string]

Paimon warehouse path.

### catalog_name [string]

The name of the Paimon catalog. Default value is `paimon`.

### catalog_type [string]

Catalog type of Paimon. Supports `filesystem` and `hive`. Default value is `filesystem`.

### catalog_uri [string]

Catalog URI of Paimon. This option is required when `catalog_type` is `hive` (for example, `thrift://hadoop04:9083`).

### database [string]

The database you want to access.

### table [string]

The table you want to access. Configure exactly one of `table` and `table_list`.

### table_list [array]

The list of tables to read. Configure exactly one of `table` and `table_list`. Each item must contain `table`, and may contain its own `query` filter.

```hocon
table_list = [
  {
    table = "table1"
    query = "select * from table1 where id > 100"
  },
  {
    table = "table2"
  }
]
```

### user [string]

The Paimon user used to access the table. Use it together with `password`. This is mainly required when the underlying catalog enforces authentication.

### password [string]

The Paimon user password. Required when `user` is configured.

### hdfs_site_path [string]

The file path of `hdfs-site.xml`. This option is deprecated; prefer `paimon.hadoop.conf` or `paimon.hadoop.conf-path` for new jobs.

### query [string]

The filter condition of the table read. For example: `select * from st_test where id > 100`. If not specified, all rows are read.
Currently, where conditions only support <, <=, >, >=, =, !=, or, and,is null, is not null, between...and, in, not in, like, and others are not supported.
The Having, Group By, Order By clauses are currently unsupported, because these clauses are not supported by Paimon.
you can also project specific columns, for example: select id, name from st_test where id > 100.

Supports dynamic options settings:
```sql
SELECT * FROM table /*+ OPTIONS('incremental-between' = 'test-tag1,test-tag2') */;
```

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
* time

### paimon.hadoop.conf [string]

Properties in hadoop conf

### paimon.hadoop.conf-path [string]

The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files

## Filesystems
The Paimon connector supports reading data from multiple file systems. Currently, the supported file systems are hdfs and s3.
If you use the s3 filesystem. You can configure the `fs.s3a.access-key`、`fs.s3a.secret-key`、`fs.s3a.endpoint`、`fs.s3a.path.style.access`、`fs.s3a.aws.credentials.provider` properties in the `paimon.hadoop.conf` option.
Besides, the warehouse should start with `s3a://`.

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

### Multiple tables

```hocon
source {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "default"
    table_list = [
      {
        database = "default"
        table = "table1"
        query = "select * from table1 where id > 100"
      },
      {
        database = "default"
        table = "table2"
        query = "select * from table2 where id > 100"
      }
    ]
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

###  S3 example
```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
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

sink {
  Console{}
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
      hadoop_user_name = "hdfs"
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

## Reading Paimon Changelog

If you want to read the changelog of the Paimon table, first set the `changelog-producer` for the Paimon source table and then use the SeaTunnel stream task to read it.

### Note

Currently, batch reads are always the latest snapshot read, so to read full changelog data, you need to use stream reads and start stream reads before writing data to the Paimon table. To ensure ordering, the parallelism of the stream read task should be set to 1.

### Streaming read example

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
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

### paimon enable privilege example

```hocon
source {
 Paimon {
     warehouse = "/tmp/paimon"
     database = "default"
     table = "st_test"
     user = "paimon"
     password = "******"
   }
}
```

## Changelog

<ChangeLog />

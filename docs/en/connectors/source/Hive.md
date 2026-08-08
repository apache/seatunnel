import ChangeLog from '../changelog/connector-hive.md';

# Hive

> Hive source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read data from Hive.

When using markdown format, SeaTunnel can parse markdown files stored in Hive tables and extract structured data with elements like headings, paragraphs, lists, code blocks, and tables. Each extracted element is converted to a document-element row with the following schema:

- `element_id`: Unique identifier for the element
- `element_type`: Type of the element (Heading, Paragraph, ListItem, etc.)
- `heading_level`: Level of heading (1-6, null for non-heading elements)
- `text`: Text content of the element
- `page_number`: Page number (default: 1)
- `position_index`: Position index within the document
- `parent_id`: ID of the parent element
- `child_ids`: Comma-separated list of child element IDs

Note: Markdown format only supports reading, not writing.

:::tip

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9 and 3.1.3 .

If you use SeaTunnel Engine, You need put seatunnel-hadoop3-3.1.4-uber.jar and hive-exec-3.1.3.jar and libfb303-0.9.3.jar in $SEATUNNEL_HOME/lib/ dir.
:::

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [schema projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] markdown

## Source Options

| Name                 | Type    | Required | Default Value  | Description                                                                                                                                                                                                                                  |
|----------------------|---------|----------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_name           | String  | No       | Required for single-table mode | Hive table name in `db.table` form. When `use_regex = true`, the value uses the `databasePattern.tablePattern` form (Hive has no schema) to match multiple tables from the Hive metastore.                                                    |
| table_list           | Array   | No       | -              | List of Hive table configurations for multi-table reading. Each item can contain `table_name`, `metastore_uri`, `use_regex`, `read_partitions`, `read_columns`, and the same authentication/Hadoop options as the root connector block.     |
| tables_configs       | Array   | No       | -              | Deprecated multi-table configuration list. New jobs should use `table_list` instead.                                                                                                                                                          |
| use_regex            | Boolean | No       | false          | Treat `table_name` as a regular expression for matching multiple tables (whole database / subset). This also works inside each entry of `table_list` / `tables_configs`.                                                                       |
| metastore_uri        | String  | No       | Required for single-table mode | Hive metastore URI. Supports comma-separated multiple URIs for HA/failover (whitespace is ignored). SeaTunnel passes this value to Hive `hive.metastore.uris` and uses `RetryingMetaStoreClient` for retry/failover.                            |
| krb5_path            | String  | No       | /etc/krb5.conf | The path of `krb5.conf`, used for Kerberos authentication.                                                                                                                                                                                    |
| kerberos_principal   | String  | No       | -              | The principal of Kerberos authentication.                                                                                                                                                                                                    |
| kerberos_keytab_path | String  | No       | -              | The keytab file path of Kerberos authentication.                                                                                                                                                                                              |
| hdfs_site_path       | String  | No       | -              | The path of `hdfs-site.xml`, used to load HA configuration of namenodes.                                                                                                                                                                       |
| hive_site_path       | String  | No       | -              | The path of `hive-site.xml`.                                                                                                                                                                                                                  |
| hive.hadoop.conf     | Map     | No       | -              | Properties in hadoop conf (`core-site.xml`, `hdfs-site.xml`, `hive-site.xml`).                                                                                                                                                               |
| hive.hadoop.conf-path| String  | No       | -              | The specified loading path for `core-site.xml`, `hdfs-site.xml`, `hive-site.xml`.                                                                                                                                                             |
| remote_user          | String  | No       | -              | Hadoop remote user name used when connecting to HDFS/Hive storage without Kerberos credentials.                                                                                                                                                |
| read_partitions      | List    | No       | -              | The target partitions to read from the Hive table. If it is not configured, all partitions are read. Every partition in the list must have the same directory depth.                                                                        |
| read_columns         | List    | No       | -              | The read column list of the data source; use it to implement field projection.                                                                                                                                                                |
| compress_codec       | String  | No       | none           | The compression codec of files. Supported codecs: `lzo`, `none` for text/json/csv. For orc/parquet, the compression type is automatically recognized and no additional setting is required.                                                  |
| common-options       |         | No       | -              | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                              |

### table_name [String]

Target Hive table name, for example `db1.table1`. When `use_regex = true`, this
field uses `databasePattern.tablePattern` (Hive has no schema) to match multiple
tables from the Hive metastore.

For a single-table source, configure `table_name` and `metastore_uri` at the root
level. For multi-table reading, configure `table_list`. `tables_configs` is still
accepted for compatibility, but `table_list` is preferred.

### table_list [Array]

List of Hive table configurations for multi-table reading. Each item can contain
`table_name`, `metastore_uri`, `use_regex`, `read_partitions`, `read_columns`, and
the same authentication/Hadoop options as the root connector block.

### tables_configs [Array]

Deprecated multi-table configuration list. New jobs should use `table_list`.

### use_regex [Boolean]

Whether to treat `table_name` as a regular expression pattern for matching
multiple tables (whole database / subset). This also works inside each entry of
`table_list` / `tables_configs`.

Regex syntax notes:

- The dot (`.`) is treated as the separator between database and table patterns
  (Hive only supports `database.table`).
- Only one unescaped dot is allowed (as the database/table separator). If you
  need to use dot (`.`) in a regular expression (e.g. `.*`), you must escape it
  as `\.` (in a HOCON string, write `\\.`).
- Examples: `db0.\.*`, `db1.user_table_[0-9]+`, `db[1-2].(app|web)order_\.*`.
- In a SeaTunnel job config (HOCON string), backslashes need escaping. For
  example, the regex `db0.\.*` should be configured as `db0.\\.*`.
- `db0.\.*` matches all tables in database `db0` (whole database
  synchronization).
- `\.*.\.*` matches all tables in all databases (whole Hive synchronization).

### metastore_uri [String]

Hive metastore URI. Supports comma-separated multiple URIs for HA/failover
(whitespace is ignored). SeaTunnel passes this value to Hive `hive.metastore.uris`
and uses Hive `RetryingMetaStoreClient` (if available) to retry/failover between
URIs. This is client-side endpoint failover; make sure your metastores
share/replicate the same backend to keep metadata consistent.

### remote_user [String]

Hadoop remote user name used when connecting to HDFS/Hive storage without
Kerberos credentials.

### hdfs_site_path [String]

The path of `hdfs-site.xml`, used to load HA configuration of namenodes.

### hive_site_path [String]

The path of `hive-site.xml`. Use this when the file is not on the default classpath.

### hive.hadoop.conf [Map]

Properties in hadoop conf (`core-site.xml`, `hdfs-site.xml`, `hive-site.xml`).

### hive.hadoop.conf-path [String]

The specified loading path for `core-site.xml`, `hdfs-site.xml`, `hive-site.xml`
files.

### read_partitions [List]

The target partitions that the user wants to read from the Hive table. If the
user does not set this parameter, all data in the Hive table is read.

**Tips: Every partition in the partition list should have the same directory
depth. For example, a Hive table has two partitions: `par1` and `par2`. The
following configuration is illegal:**

```
read_partitions = [par1=xxx, par1=yyy/par2=zzz]
```

### krb5_path [String]

The path of `krb5.conf`, used for Kerberos authentication.

### kerberos_principal [String]

The principal of Kerberos authentication.

### kerberos_keytab_path [String]

The keytab file path of Kerberos authentication.

### read_columns [List]

The read column list of the data source; the user can use it to implement field
projection.

### compress_codec [String]

The compression codec of files:

- `text`/`json`/`csv`: `lzo`, `none`
- `orc`/`parquet`: automatically recognizes the compression type, no additional
  settings required.

### common options

Source plugin common parameters, please refer to
[Source Common Options](../common-options/source-common-options.md) for details.

## Task Example

### Example 1: Single table

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }
}

sink {
  Console {}
}
```

### Example 2: Metastore URI failover

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://metastore-1:9083,thrift://metastore-2:9083"
  }
}
```

### Example 3: Multiple tables

> Note: Hive is a structured data source and should use `table_list`; `tables_configs` will be removed in the future. You can also set `use_regex = true` in each table config to match multiple tables.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Hive {
    table_list = [
      {
        table_name = "default.seatunnel_orc_1"
        metastore_uri = "thrift://namenode001:9083"
      },
      {
        table_name = "default.seatunnel_orc_2"
        metastore_uri = "thrift://namenode001:9083"
      }
    ]
  }
}
```

Deprecated `tables_configs` form (still accepted for backward compatibility):

```hocon
source {
  Hive {
    tables_configs = [
      {
        table_name = "default.seatunnel_orc_1"
        metastore_uri = "thrift://namenode001:9083"
      },
      {
        table_name = "default.seatunnel_orc_2"
        metastore_uri = "thrift://namenode001:9083"
      }
    ]
  }
}
```

### Example 4: Regex matching (whole database / subset)

Whole database (`a`):

```hocon
source {
  Hive {
    metastore_uri = "thrift://namenode001:9083"
    table_name = "a.\\.*"
    use_regex = true
  }
}
```

Whole Hive (all databases):

```hocon
source {
  Hive {
    metastore_uri = "thrift://namenode001:9083"
    table_name = "\\.*.\\.*"
    use_regex = true
  }
}
```

Subset (tables matching `tmp_.*` in database `a`). Note: escape the dot
wildcard as `\.` (in a HOCON string, write `\\.`) because unescaped dots are
treated as separators.

```hocon
source {
  Hive {
    metastore_uri = "thrift://namenode001:9083"
    table_name = "a.tmp_\\.*"
    use_regex = true
  }
}
```

### Example 5: Kerberos

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Hive {
    table_name = "default.test_hive_sink_on_hdfs_with_kerberos"
    metastore_uri = "thrift://metastore:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    hive_site_path = "/tmp/hive-site.xml"
    kerberos_principal = "hive/metastore.seatunnel@EXAMPLE.COM"
    kerberos_keytab_path = "/tmp/hive.keytab"
    krb5_path = "/tmp/krb5.conf"
  }
}

sink {
  Assert {
    plugin_input = hive_source
    rules {
      row_rules = [
        { rule_type = MAX_ROW, rule_value = 3 }
      ]
      field_rules = [
        { field_name = pk_id, field_type = bigint, field_value = [{ rule_type = NOT_NULL }] },
        { field_name = name, field_type = string, field_value = [{ rule_type = NOT_NULL }] },
        { field_name = score, field_type = int, field_value = [{ rule_type = NOT_NULL }] }
      ]
    }
  }
}
```

Description of the Kerberos options:

- `hive_site_path`: The path to the `hive-site.xml` file.
- `kerberos_principal`: The principal for Kerberos authentication.
- `kerberos_keytab_path`: The keytab file path for Kerberos authentication.
- `krb5_path`: The path to the `krb5.conf` file used for Kerberos authentication.

## Hive on s3

### Step 1: Create the lib dir for Hive

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 2: Download jars from Maven Central

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.6.5/hadoop-aws-2.6.5.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### Step 3: Copy EMR jars into the lib dir

```shell
cp /usr/share/aws/emr/emrfs/lib/emrfs-hadoop-assembly-2.60.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/hadoop-common-3.3.6-amzn-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/javax.inject-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/aopalliance-1.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 4: Run the job

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Hive {
    table_name = "test_hive.test_hive_sink_on_s3"
    metastore_uri = "thrift://ip-192-168-0-202.cn-north-1.compute.internal:9083"
    hive.hadoop.conf-path = "/home/ec2-user/hadoop-conf"
    hive.hadoop.conf = {
      bucket = "s3://ws-package"
      fs.s3a.aws.credentials.provider = "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    }
    read_columns = ["pk_id", "name", "score"]
  }
}

sink {
  Hive {
    table_name = "test_hive.test_hive_sink_on_s3_sink"
    metastore_uri = "thrift://ip-192-168-0-202.cn-north-1.compute.internal:9083"
    hive.hadoop.conf-path = "/home/ec2-user/hadoop-conf"
    hive.hadoop.conf = {
      bucket = "s3://ws-package"
      fs.s3a.aws.credentials.provider = "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    }
  }
}
```

## Hive on oss

### Step 1: Create the lib dir for Hive

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 2: Download jars from Maven Central

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### Step 3: Copy JindoSDK jars and remove conflicting Hadoop Aliyun jars

```shell
cp -r /opt/apps/JINDOSDK/jindosdk-current/lib/jindo-*.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
rm -f ${SEATUNNEL_HOME}/lib/hadoop-aliyun-*.jar
```

### Step 4: Run the job

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Hive {
    table_name = "test_hive.test_hive_sink_on_oss"
    metastore_uri = "thrift://master-1-1.c-1009b01725b501f2.cn-wulanchabu.emr.aliyuncs.com:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    hive.hadoop.conf = {
      bucket = "oss://emr-osshdfs.cn-wulanchabu.oss-dls.aliyuncs.com"
    }
  }
}

sink {
  Hive {
    table_name = "test_hive.test_hive_sink_on_oss_sink"
    metastore_uri = "thrift://master-1-1.c-1009b01725b501f2.cn-wulanchabu.emr.aliyuncs.com:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    hive.hadoop.conf = {
      bucket = "oss://emr-osshdfs.cn-wulanchabu.oss-dls.aliyuncs.com"
    }
  }
}
```

## Changelog

<ChangeLog />
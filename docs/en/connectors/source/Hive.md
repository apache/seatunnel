import ChangeLog from '../changelog/connector-hive.md';

# Hive

> Hive source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read data from Apache Hive tables. The connector talks to Hive Metastore for schema discovery and reads the underlying files from HDFS (or S3/OSS when configured). The supported file formats include text, CSV, parquet, ORC, JSON, and markdown. Each table can be read as one batch split, with parallelism and snapshot/offset resume supported through the checkpoint mechanism.

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

If you use SeaTunnel Engine, You need put seatunnel-shade-hadoop3-uber-3.1.4-3.0.0.jar and hive-exec-3.1.3.jar and libfb303-0.9.3.jar in $SEATUNNEL_HOME/lib/ dir.
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

## Options

|         name          |  type  | required | default value  | description |
|-----------------------|--------|----------|----------------|-------------|
| table_name            | string | no       | Required for single-table mode | Target Hive table name in the form `db1.table1`. When `use_regex = true`, this field uses `databasePattern.tablePattern` to match multiple tables. |
| table_list            | array  | no       | Deprecated, use `tables_configs` instead | Deprecated multi-table configuration list. New jobs should use `tables_configs`. Kept for backward compatibility; will be removed in a future release. |
| tables_configs        | array  | no       | -              | List of Hive table configurations for multi-table reading. Each item can override any of the root-level options. |
| use_regex             | boolean| no       | false          | Treat `table_name` as a regular expression that matches multiple tables. Works at the root level and inside each `table_list` / `tables_configs` entry. |
| metastore_uri         | string | no       | Required for single-table mode | Hive metastore URI. Comma-separated values enable HA failover; whitespace is ignored. |
| krb5_path             | string | no       | /etc/krb5.conf | Path of the `krb5.conf` file used for Kerberos authentication. |
| kerberos_principal    | string | no       | -              | Principal for Kerberos authentication against Hive Metastore / HDFS. |
| kerberos_keytab_path  | string | no       | -              | Path to the keytab file paired with `kerberos_principal`. |
| hdfs_site_path        | string | no       | -              | Local path of `hdfs-site.xml`. Used to load HDFS HA configuration. Deprecated for new jobs — prefer `hive.hadoop.conf` or `hive.hadoop.conf-path`. |
| hive_site_path        | string | no       | -              | Local path of `hive-site.xml`. |
| hive.hadoop.conf      | Map    | no       | -              | Inline Hadoop configuration properties (equivalent to entries from `core-site.xml` / `hdfs-site.xml` / `hive-site.xml`). |
| hive.hadoop.conf-path | string | no       | -              | Directory that contains `core-site.xml`, `hdfs-site.xml`, and `hive-site.xml`. |
| remote_user           | string | no       | -              | Hadoop remote user name used when connecting to HDFS / Hive storage without Kerberos. |
| read_partitions       | list   | no       | -              | Restrict the read to a subset of partitions. All entries must have the same directory depth. |
| read_columns          | list   | no       | -              | Column projection list. Only the listed columns are read from the source. |
| compress_codec        | string | no       | none           | Compression codec for text / CSV / JSON outputs. `lzo` and `none` are supported. Parquet / ORC auto-detect compression. |
| common-options        |        | no       | -              | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

### table_name [string]

Target Hive table name eg: `db1.table1`. When `use_regex = true`, this field uses `databasePattern.tablePattern` (Hive has no schema) to match multiple tables from Hive metastore.

For a single-table source, configure `table_name` and `metastore_uri` at the root level. For multi-table reading, configure `tables_configs`. `table_list` is still accepted for backward compatibility, but `tables_configs` is the current option.

### table_list [array]

Deprecated multi-table configuration list. Kept for backward compatibility; new jobs should use `tables_configs`.

### tables_configs [array]

List of Hive table configurations for multi-table reading. Each item can contain `table_name`, `metastore_uri`, `use_regex`, `read_partitions`, `read_columns`, and the same authentication/Hadoop options as the root connector block.

### use_regex [boolean]

Whether to treat `table_name` as a regular expression pattern for matching multiple tables (whole database / subset). This also works inside each entry of `table_list` / `tables_configs`.

Regex syntax notes:
- The dot (`.`) is treated as the separator between database and table patterns (Hive only supports `database.table`).
- Only one unescaped dot is allowed (as the database/table separator). If you need to use dot (`.`) in a regular expression (e.g. `.*`), you must escape it as `\.` (in a HOCON string, write `\\.`).
- Examples: `db0.\.*`, `db1.user_table_[0-9]+`, `db[1-2].(app|web)order_\.*`.
- In SeaTunnel job config (HOCON string), backslashes need escaping. For example, the regex `db0.\.*` should be configured as `db0.\\.*`.
- `db0.\.*` matches all tables in database `db0` (whole database synchronization).
- `\.*.\.*` matches all tables in all databases (whole Hive synchronization).

### metastore_uri [string]

Hive metastore uri. Supports comma-separated multiple URIs for HA/failover (whitespace is ignored). SeaTunnel passes this value to Hive `hive.metastore.uris` and uses Hive `RetryingMetaStoreClient` (if available) to retry/failover between URIs. This is client-side endpoint failover; make sure your metastores share/replicate the same backend to keep metadata consistent.

### remote_user [string]

Hadoop remote user name used when connecting to HDFS/Hive storage without Kerberos credentials.

### hdfs_site_path [string]

The path of `hdfs-site.xml`, used to load ha configuration of namenodes

### hive.hadoop.conf [map]

Properties in hadoop conf('core-site.xml', 'hdfs-site.xml', 'hive-site.xml')

### hive.hadoop.conf-path [string]

The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files

### read_partitions [list]

The target partitions that user want to read from hive table, if user does not set this parameter, it will read all the data from hive table.

**Tips: Every partition in partitions list should have the same directory depth. For example, a hive table has two partitions: par1 and par2, if user sets it like as the following:**
**read_partitions = [par1=xxx, par1=yyy/par2=zzz], it is illegal**

### krb5_path [string]

The path of `krb5.conf`, used to authentication kerberos

### kerberos_principal [string]

The principal of kerberos authentication

### kerberos_keytab_path [string]

The keytab file path of kerberos authentication

### read_columns [list]

The read column list of the data source, user can use it to implement field projection.

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details

## Example

### Example 1: Single table

```hocon

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

### Example 2: Metastore URI failover

```hocon
  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://metastore-1:9083,thrift://metastore-2:9083"
  }
```

### Example 3: Multiple tables
> Note: Hive is a structured data source and should use 'tables_configs'; the older 'table_list' key is deprecated and will be removed in a future release.
> You can also set `use_regex = true` in each table config to match multiple tables.

```hocon

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

```

### Example 4: Regex matching (whole database / subset)

```hocon
  Hive {
    metastore_uri = "thrift://namenode001:9083"

    # 1) Whole database: all tables in database `a`
    table_name = "a.\\.*"
    use_regex = true
  }
```

```hocon
  Hive {
    metastore_uri = "thrift://namenode001:9083"

    # 2) Whole Hive: all tables in all databases
    table_name = "\\.*.\\.*"
    use_regex = true
  }
```

```hocon
  Hive {
    metastore_uri = "thrift://namenode001:9083"

    # 3) Subset: tables matching `tmp_.*` in database `a`
    #    Note: escape the dot wildcard as `\.` (in HOCON string, write `\\.`) because unescaped dots are treated as separators
    table_name = "a.tmp_\\.*"
    use_regex = true
  }
```

### Example 5: Kerberos

```hocon
source {
  Hive {
    table_name = "default.test_hive_sink_on_hdfs_with_kerberos"
    metastore_uri = "thrift://metastore:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    plugin_output = hive_source
    hive_site_path = "/tmp/hive-site.xml"
    kerberos_principal = "hive/metastore.seatunnel@EXAMPLE.COM"
    kerberos_keytab_path = "/tmp/hive.keytab"
    krb5_path = "/tmp/krb5.conf"
  }
}
```

Description:

- `hive_site_path`: The path to the `hive-site.xml` file.
- `kerberos_principal`: The principal for Kerberos authentication.
- `kerberos_keytab_path`: The keytab file path for Kerberos authentication.
- `krb5_path`: The path to the `krb5.conf` file used for Kerberos authentication.

Run the case:

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
    plugin_output = hive_source
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
        {
          rule_type = MAX_ROW
          rule_value = 3
        }
      ],
      field_rules = [
        {
          field_name = pk_id
          field_type = bigint
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        },
        {
          field_name = name
          field_type = string
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        },
        {
          field_name = score
          field_type = int
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        }
      ]
    }
  }
}
```

## Hive on s3

### Step 1

Create the lib dir for hive of emr.

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 2

Get the jars from maven center to the lib.

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.6.5/hadoop-aws-2.6.5.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### Step 3

Copy the jars from your environment on emr to the lib dir.

```shell
cp /usr/share/aws/emr/emrfs/lib/emrfs-hadoop-assembly-2.60.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/hadoop-common-3.3.6-amzn-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/javax.inject-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/aopalliance-1.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 4

Run the case.

```shell
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
       bucket="s3://ws-package"
       fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
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
       bucket="s3://ws-package"
       fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    }
  }
}
```

## Hive on oss

### Step 1

Create the lib dir for hive of emr.

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 2

Get the jars from maven center to the lib.

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### Step 3

Copy the jars from your environment on emr to the lib dir and delete the conflicting jar.

```shell
cp -r /opt/apps/JINDOSDK/jindosdk-current/lib/jindo-*.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
rm -f ${SEATUNNEL_HOME}/lib/hadoop-aliyun-*.jar
```

### Step 4

Run the case.

```shell
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
        bucket="oss://emr-osshdfs.cn-wulanchabu.oss-dls.aliyuncs.com"
    }
  }
}

sink {
  Hive {
    table_name = "test_hive.test_hive_sink_on_oss_sink"
    metastore_uri = "thrift://master-1-1.c-1009b01725b501f2.cn-wulanchabu.emr.aliyuncs.com:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    hive.hadoop.conf = {
        bucket="oss://emr-osshdfs.cn-wulanchabu.oss-dls.aliyuncs.com"
    }
  }
}
```

## Changelog

<ChangeLog />

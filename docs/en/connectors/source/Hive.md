import ChangeLog from '../changelog/connector-hive.md';

# Hive

> Hive source connector

## Description

Read data from Hive.

When using markdown format, SeaTunnel can parse markdown files stored in Hive tables and extract structured data with elements like headings, paragraphs, lists, code blocks, and tables. Each element is converted to a row with the following schema:
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

## Options

|         name          |  type  | required | default value  |
|-----------------------|--------|----------|----------------|
| table_name            | string | yes      | -              |
| use_regex             | boolean| no       | false          |
| metastore_uri         | string | yes      | -              |
| krb5_path             | string | no       | /etc/krb5.conf |
| kerberos_principal    | string | no       | -              |
| kerberos_keytab_path  | string | no       | -              |
| hdfs_site_path        | string | no       | -              |
| hive_site_path        | string | no       | -              |
| hive.hadoop.conf      | Map    | no       | -              |
| hive.hadoop.conf-path | string | no       | -              |
| read_partitions       | list   | no       | -              |
| read_columns          | list   | no       | -              |
| compress_codec        | string | no       | none           |
| common-options        |        | no       | -              |

### table_name [string]

Target Hive table name eg: `db1.table1`. When `use_regex = true`, this field uses `databasePattern.tablePattern` (Hive has no schema) to match multiple tables from Hive metastore.

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

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

### Example 2: Metastore URI failover

```bash
  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://metastore-1:9083,thrift://metastore-2:9083"
  }
```

### Example 3: Multiple tables
> Note: Hive is a structured data source and should be use 'table_list', and 'tables_configs' will be removed in the future.
> You can also set `use_regex = true` in each table config to match multiple tables.

```bash

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

```

```bash

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

### Example 3: Regex matching (whole database / subset)

```bash
  Hive {
    metastore_uri = "thrift://namenode001:9083"

    # 1) Whole database: all tables in database `a`
    table_name = "a.\\.*"
    use_regex = true
  }
```

```bash
  Hive {
    metastore_uri = "thrift://namenode001:9083"

    # 2) Whole Hive: all tables in all databases
    table_name = "\\.*.\\.*"
    use_regex = true
  }
```

```bash
  Hive {
    metastore_uri = "thrift://namenode001:9083"

    # 3) Subset: tables matching `tmp_.*` in database `a`
    #    Note: escape the dot wildcard as `\.` (in HOCON string, write `\\.`) because unescaped dots are treated as separators
    table_name = "a.tmp_\\.*"
    use_regex = true
  }
```

### Example 4 : Kerberos

```bash
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

```bash
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

# Hive

> Hive source connector

## Description

Read data from Hive.

:::tip

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

If you use SeaTunnel Engine, You need put seatunnel-hadoop3-3.1.4-uber.jar and hive-exec-3.1.3.jar and libfb303-0.9.3.jar in $SEATUNNEL_HOME/lib/ dir.
:::

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)

Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## Options

|         name          |  type  | required | default value  |
|-----------------------|--------|----------|----------------|
| table_name            | string | yes      | -              |
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

Target Hive table name eg: db1.table1

### metastore_uri [string]

Hive metastore uri

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

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details

## Example

### Example 1: Single table

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

### Example 2: Multiple tables

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

### 2.2.0-beta 2022-09-26

- Add Hive Source Connector

### Next version

- [Improve] Support kerberos authentication ([3840](https://github.com/apache/seatunnel/pull/3840))
- Support user-defined partitions ([3842](https://github.com/apache/seatunnel/pull/3842))


# Hive

> Hive sink connector

## Description

Write data to Hive.

:::tip

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9 and 3.1.3 .

If you use SeaTunnel Engine, You need put seatunnel-hadoop3-3.1.4-uber.jar and hive-exec-3.1.3.jar and libfb303-0.9.3.jar in $SEATUNNEL_HOME/lib/ dir.
:::

## Key features

- [x] [support multiple table write](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
- [x] compress codec
  - [x] lzo

## Options

| name                                  | type    | required | default value  |
|---------------------------------------|---------|----------|----------------|
| table_name                            | string  | yes      | -              |
| metastore_uri                         | string  | yes      | -              |
| compress_codec                        | string  | no       | none           |
| hdfs_site_path                        | string  | no       | -              |
| hive_site_path                        | string  | no       | -              |
| hive.hadoop.conf                      | Map     | no       | -              |
| hive.hadoop.conf-path                 | string  | no       | -              |
| krb5_path                             | string  | no       | /etc/krb5.conf |
| kerberos_principal                    | string  | no       | -              |
| kerberos_keytab_path                  | string  | no       | -              |
| abort_drop_partition_metadata         | boolean | no       | true           |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false          |
| common-options                        |         | no       | -              |

### table_name [string]

Target Hive table name eg: db1.table1, and if the source is multiple mode, you can use `${database_name}.${table_name}` to generate the table name, it will replace the `${database_name}` and `${table_name}` with the value of the CatalogTable generate from the source.

### metastore_uri [string]

Hive metastore uri

### hdfs_site_path [string]

The path of `hdfs-site.xml`, used to load ha configuration of namenodes

### hive_site_path [string]

The path of `hive-site.xml`

### hive.hadoop.conf [map]

Properties in hadoop conf('core-site.xml', 'hdfs-site.xml', 'hive-site.xml')

### hive.hadoop.conf-path [string]

The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files

### krb5_path [string]

The path of `krb5.conf`, used to authentication kerberos

The path of `hive-site.xml`, used to authentication hive metastore

### kerberos_principal [string]

The principal of kerberos

### kerberos_keytab_path [string]

The keytab path of kerberos

### abort_drop_partition_metadata [boolean]

Flag to decide whether to drop partition metadata from Hive Metastore during an abort operation. Note: this only affects the metadata in the metastore, the data in the partition will always be deleted(data generated during the synchronization process).

### parquet_avro_write_timestamp_as_int96 [boolean]

Support writing Parquet INT96 from a timestamp, only valid for parquet files.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details

## Example

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

### example 1

We have a source table like this:

```bash
create table test_hive_source(
     test_tinyint                          TINYINT,
     test_smallint                       SMALLINT,
     test_int                                INT,
     test_bigint                           BIGINT,
     test_boolean                       BOOLEAN,
     test_float                             FLOAT,
     test_double                         DOUBLE,
     test_string                           STRING,
     test_binary                          BINARY,
     test_timestamp                  TIMESTAMP,
     test_decimal                       DECIMAL(8,2),
     test_char                             CHAR(64),
     test_varchar                        VARCHAR(64),
     test_date                             DATE,
     test_array                            ARRAY<INT>,
     test_map                              MAP<STRING, FLOAT>,
     test_struct                           STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
     )
PARTITIONED BY (test_par1 STRING, test_par2 STRING);

```

We need read data from the source table and write to another table:

```bash
create table test_hive_sink_text_simple(
     test_tinyint                          TINYINT,
     test_smallint                       SMALLINT,
     test_int                                INT,
     test_bigint                           BIGINT,
     test_boolean                       BOOLEAN,
     test_float                             FLOAT,
     test_double                         DOUBLE,
     test_string                           STRING,
     test_binary                          BINARY,
     test_timestamp                  TIMESTAMP,
     test_decimal                       DECIMAL(8,2),
     test_char                             CHAR(64),
     test_varchar                        VARCHAR(64),
     test_date                             DATE
     )
PARTITIONED BY (test_par1 STRING, test_par2 STRING);

```

The job config file can like this:

```
env {
  parallelism = 3
  job.name="test_hive_source_to_hive"
}

source {
  Hive {
    table_name = "test_hive.test_hive_source"
    metastore_uri = "thrift://ctyun7:9083"
  }
}

sink {
  # choose stdout output plugin to output data to console

  Hive {
    table_name = "test_hive.test_hive_sink_text_simple"
    metastore_uri = "thrift://ctyun7:9083"
    hive.hadoop.conf = {
      bucket = "s3a://mybucket"
      fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    }
}
```

### example2: Kerberos

```bash
sink {
  Hive {
    table_name = "default.test_hive_sink_on_hdfs_with_kerberos"
    metastore_uri = "thrift://metastore:9083"
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
      }
    ]
  }
}

sink {
  Hive {
    table_name = "default.test_hive_sink_on_hdfs_with_kerberos"
    metastore_uri = "thrift://metastore:9083"
    hive_site_path = "/tmp/hive-site.xml"
    kerberos_principal = "hive/metastore.seatunnel@EXAMPLE.COM"
    kerberos_keytab_path = "/tmp/hive.keytab"
    krb5_path = "/tmp/krb5.conf"
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
      }
    ]
  }
}

sink {
  Hive {
    table_name = "test_hive.test_hive_sink_on_s3"
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
      }
    ]
  }
}

sink {
  Hive {
    table_name = "test_hive.test_hive_sink_on_oss"
    metastore_uri = "thrift://master-1-1.c-1009b01725b501f2.cn-wulanchabu.emr.aliyuncs.com:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    hive.hadoop.conf = {
        bucket="oss://emr-osshdfs.cn-wulanchabu.oss-dls.aliyuncs.com"
    }
  }
}
```

### example 2

We have multiple source table like this:

```bash
create table test_1(
)
PARTITIONED BY (xx);

create table test_2(
)
PARTITIONED BY (xx);
...
```

We need read data from these source tables and write to another tables:

The job config file can like this:

```
env {
  # You can set flink configuration here
  parallelism = 3
  job.name="test_hive_source_to_hive"
}

source {
  Hive {
    tables_configs = [
      {
        table_name = "test_hive.test_1"
        metastore_uri = "thrift://ctyun6:9083"
      },
      {
        table_name = "test_hive.test_2"
        metastore_uri = "thrift://ctyun7:9083"
      }
    ]
  }
}

sink {
  # choose stdout output plugin to output data to console
  Hive {
    table_name = "${database_name}.${table_name}"
    metastore_uri = "thrift://ctyun7:9083"
  }
}
```

import ChangeLog from '../changelog/connector-hive.md';

# Hive

> Hive sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Hive.

:::tip

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9 and 3.1.3 .

If you use SeaTunnel Engine, You need put seatunnel-hadoop3-3.1.4-uber.jar and hive-exec-3.1.3.jar and libfb303-0.9.3.jar in $SEATUNNEL_HOME/lib/ dir.
:::

## Key features

- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`.

- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
- [x] compress codec
  - [x] lzo

## Sink Options

| Name                                | Type    | Required | Default Value                     | Description                                                                                                                                                                                                                                                                                                                                       |
|-------------------------------------|---------|----------|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_name                          | String  | Yes      | -                                 | Target Hive table name, for example `db1.table1`. When the source is in multi-table mode, you can use `${database_name}.${table_name}` to generate the table name; `${database_name}` and `${table_name}` are replaced with the values from the upstream `CatalogTable`.                                                                       |
| metastore_uri                       | String  | Yes      | -                                 | Hive metastore URI. Supports comma-separated multiple URIs for HA/failover (whitespace is ignored). SeaTunnel passes this value to Hive `hive.metastore.uris` and uses `RetryingMetaStoreClient` to retry/failover between URIs.                                                                                                          |
| compress_codec                      | String  | No       | none                              | The compression codec of files. Supported codecs: `lzo`, `none` for text/csv/json. For orc/parquet the compression type is automatically recognized from the file metadata.                                                                                                                                                                     |
| hdfs_site_path                      | String  | No       | -                                 | The path of `hdfs-site.xml`, used to load HA configuration of namenodes.                                                                                                                                                                                                                                                                          |
| hive_site_path                      | String  | No       | -                                 | The path of `hive-site.xml`.                                                                                                                                                                                                                                                                                                                       |
| hive.hadoop.conf                    | Map     | No       | -                                 | Properties in hadoop conf (`core-site.xml`, `hdfs-site.xml`, `hive-site.xml`).                                                                                                                                                                                                                                                                    |
| hive.hadoop.conf-path               | String  | No       | -                                 | The specified loading path for `core-site.xml`, `hdfs-site.xml`, `hive-site.xml`.                                                                                                                                                                                                                                                                  |
| remote_user                         | String  | No       | -                                 | Hadoop remote user name used when connecting to HDFS/Hive storage without Kerberos credentials.                                                                                                                                                                                                                                                   |
| krb5_path                           | String  | No       | /etc/krb5.conf                     | The path of `krb5.conf`, used for Kerberos authentication.                                                                                                                                                                                                                                                                                        |
| kerberos_principal                  | String  | No       | -                                 | The principal of Kerberos authentication.                                                                                                                                                                                                                                                                                                         |
| kerberos_keytab_path                | String  | No       | -                                 | The keytab file path of Kerberos authentication.                                                                                                                                                                                                                                                                                                  |
| abort_drop_partition_metadata       | Boolean | No       | false                             | Drop partition metadata from the Hive Metastore during an abort operation. Only affects metastore metadata; the data in the partition is always deleted (data generated during the synchronization process).                                                                                                                                   |
| parquet_avro_write_timestamp_as_int96 | Boolean | No       | false                             | Support writing Parquet INT96 from a timestamp. Only valid for Parquet files.                                                                                                                                                                                                                                                                     |
| overwrite                           | Boolean | No       | false                             | Use overwrite mode when inserting data into Hive. For non-partitioned tables, the existing data in the table is deleted before inserting new data. For partitioned tables, the data in the relevant partition is deleted before inserting new data.                                                                                            |
| data_save_mode                      | Enum    | No       | APPEND_DATA                       | How to handle existing data on the target before writing new data. `APPEND_DATA` keeps existing data and appends new records. `DROP_DATA` behaves like `overwrite = true` and is the recommended option when you want to fully replace the table. `CUSTOM_PROCESSING` and `ERROR_WHEN_DATA_EXISTS` are not recommended unless you have specific requirements. |
| schema_save_mode                    | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST       | How to handle the existing table structure on the target before the synchronization task starts. See `schema_save_mode` below for valid values.                                                                                                                                                                                                  |
| save_mode_create_template           | String  | No       | -                                 | Template used to auto-create the Hive table. Available template variables: `${database}`, `${table}`, `${rowtype_fields}`, `${rowtype_partition_fields}`, `${table_location}`.                                                                                                                                                                    |
| common-options                      |         | No       | -                                 | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                                                                                                                                                                                       |

### table_name [String]

Target Hive table name, for example `db1.table1`. When the source is in
multi-table mode, you can use `${database_name}.${table_name}` to generate the
table name; `${database_name}` and `${table_name}` are replaced with the
values from the upstream `CatalogTable`.

### metastore_uri [String]

Hive metastore URI. Supports comma-separated multiple URIs for HA/failover
(whitespace is ignored). SeaTunnel passes this value to Hive `hive.metastore.uris`
and uses `RetryingMetaStoreClient` (if available) to retry/failover between
URIs. This is client-side endpoint failover; make sure your metastores
share/replicate the same backend to keep metadata consistent.

### hdfs_site_path [String]

The path of `hdfs-site.xml`, used to load HA configuration of namenodes.

### hive_site_path [String]

The path of `hive-site.xml`.

### hive.hadoop.conf [Map]

Properties in hadoop conf (`core-site.xml`, `hdfs-site.xml`, `hive-site.xml`).

### hive.hadoop.conf-path [String]

The specified loading path for `core-site.xml`, `hdfs-site.xml`, `hive-site.xml`
files.

### remote_user [String]

Hadoop remote user name used when connecting to HDFS/Hive storage without
Kerberos credentials.

### krb5_path [String]

The path of `krb5.conf`, used for Kerberos authentication.

### kerberos_principal [String]

The principal of Kerberos authentication.

### kerberos_keytab_path [String]

The keytab file path of Kerberos authentication.

### abort_drop_partition_metadata [Boolean]

Whether to drop partition metadata from the Hive Metastore during an abort
operation. Note: this only affects the metadata in the metastore, the data in
the partition will always be deleted (data generated during the synchronization
process).

The default value is `false`.

### parquet_avro_write_timestamp_as_int96 [Boolean]

Support writing Parquet INT96 from a timestamp. Only valid for Parquet files.

### overwrite [Boolean]

Whether to use overwrite mode when inserting data into Hive.

- For non-partitioned tables, the existing data in the table is deleted before
  inserting new data.
- For partitioned tables, the data in the relevant partition is deleted before
  inserting new data.

Behavior by job mode:

- **Batch mode (`BATCH`)**: Delete existing data in the target path before
  commit (for non-partitioned tables, delete the table directory; for
  partitioned tables, delete the related partition directories), then write new
  data.
- **Streaming mode (`STREAMING`)**: In streaming jobs with checkpointing
  enabled, `commit()` is invoked after each completed checkpoint. To avoid
  deleting on every checkpoint (which would wipe previously committed files),
  SeaTunnel deletes each target directory (table directory / partition
  directory) at most once (empty commits will skip deletion). On recovery, the
  delete step is best-effort and may be skipped to avoid deleting already
  committed data, so streaming overwrite is not a strict snapshot overwrite.

### data_save_mode [Enum]

How to handle existing data on the target before writing new data.

- `APPEND_DATA` (default): Keep existing data and append new records.
- `DROP_DATA`: Behaves the same as `overwrite = true`. Before commit, delete
  the existing data in the target path (for non-partitioned tables, delete the
  table directory; for partitioned tables, delete the related partition
  directories), then write new data.
- `CUSTOM_PROCESSING` / `ERROR_WHEN_DATA_EXISTS`: Currently not recommended for
  Hive sink unless you have specific requirements.

Note: `overwrite = true` and `data_save_mode = "DROP_DATA"` are equivalent. Use
either one; do not set both.

For batch jobs, use either `overwrite = true` or `data_save_mode = "DROP_DATA"`
when the target Hive table should be replaced by the current run. For normal
append jobs, keep the default `data_save_mode = "APPEND_DATA"`.

### schema_save_mode [Enum]

How to handle the existing table structure on the target before the
synchronization task starts.

**Default value**: `CREATE_SCHEMA_WHEN_NOT_EXIST`

Option values:

- `RECREATE_SCHEMA`: Create the table when it does not exist, drop and rebuild
  it when the table exists.
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Create the table when it does not exist,
  skip when the table exists.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Report an error when the table does not exist.
- `IGNORE`: Skip table handling.

### save_mode_create_template [String]

Use templates to automatically create Hive tables; the connector renders the
upstream row type into the template. Available template variables:
`${database}`, `${table}`, `${rowtype_fields}`, `${rowtype_partition_fields}`,
`${table_location}`.

**Default value**: When not specified, the connector uses a default
non-partitioned PARQUET table template:

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
  ${rowtype_fields}
)
STORED AS PARQUET
LOCATION '${table_location}'
```

### common options

Sink plugin common parameters, please refer to
[Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Example 1: Single table read & write

We have a source table like this:

```sql
create table test_hive_source(
     test_tinyint   TINYINT,
     test_smallint  SMALLINT,
     test_int       INT,
     test_bigint    BIGINT,
     test_boolean   BOOLEAN,
     test_float     FLOAT,
     test_double    DOUBLE,
     test_string    STRING,
     test_binary    BINARY,
     test_timestamp TIMESTAMP,
     test_decimal   DECIMAL(8,2),
     test_char      CHAR(64),
     test_varchar   VARCHAR(64),
     test_date      DATE,
     test_array     ARRAY<INT>,
     test_map       MAP<STRING, FLOAT>,
     test_struct    STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
PARTITIONED BY (test_par1 STRING, test_par2 STRING);
```

We read from this source table and write to a sink table:

```sql
create table test_hive_sink_text_simple(
     test_tinyint   TINYINT,
     test_smallint  SMALLINT,
     test_int       INT,
     test_bigint    BIGINT,
     test_boolean   BOOLEAN,
     test_float     FLOAT,
     test_double    DOUBLE,
     test_string    STRING,
     test_binary    BINARY,
     test_timestamp TIMESTAMP,
     test_decimal   DECIMAL(8,2),
     test_char      CHAR(64),
     test_varchar   VARCHAR(64),
     test_date      DATE
)
PARTITIONED BY (test_par1 STRING, test_par2 STRING);
```

```hocon
env {
  parallelism = 3
  job.name = "test_hive_source_to_hive"
  job.mode = "BATCH"
}

source {
  Hive {
    table_name = "test_hive.test_hive_source"
    metastore_uri = "thrift://ctyun7:9083"
  }
}

sink {
  Hive {
    table_name = "test_hive.test_hive_sink_text_simple"
    metastore_uri = "thrift://ctyun7:9083"
    hive.hadoop.conf = {
      bucket = "s3a://mybucket"
      fs.s3a.aws.credentials.provider = "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    }
  }
}
```

### Example 2: Kerberos

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
      { kind = INSERT, fields = [1, "A", 100] },
      { kind = INSERT, fields = [2, "B", 100] },
      { kind = INSERT, fields = [3, "C", 100] }
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

Description:

- `hive_site_path`: The path to the `hive-site.xml` file.
- `kerberos_principal`: The principal for Kerberos authentication.
- `kerberos_keytab_path`: The keytab file path for Kerberos authentication.
- `krb5_path`: The path to the `krb5.conf` file used for Kerberos authentication.

### Example 3: Multiple tables (multi-table write)

When the source produces multiple tables, use `${database_name}.${table_name}`
placeholders so each upstream table is written to a matching Hive table.

```hocon
env {
  parallelism = 3
  job.mode = "BATCH"
}

source {
  Hive {
    table_list = [
      { table_name = "test_hive.test_1", metastore_uri = "thrift://ctyun6:9083" },
      { table_name = "test_hive.test_2", metastore_uri = "thrift://ctyun7:9083" }
    ]
  }
}

sink {
  Hive {
    table_name = "${database_name}.${table_name}"
    metastore_uri = "thrift://ctyun7:9083"
  }
}
```

### Example 4: Auto Table Creation

Use `save_mode_create_template` together with `schema_save_mode =
"CREATE_SCHEMA_WHEN_NOT_EXIST"` to let the connector create the Hive table on
the fly:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = bigint
        name = string
        department = string
        salary = decimal(10,2)
        hire_date = date
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "John Doe", "Engineering", 75000.50, "2022-01-15"]
      }
    ]
  }
}

sink {
  Hive {
    table_name = "warehouse.employees"
    metastore_uri = "thrift://metastore:9083"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    save_mode_create_template = """
      CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
        ${rowtype_fields}
      )
      PARTITIONED BY (
        department string COMMENT 'Department partition'
      )
      STORED AS PARQUET
      LOCATION '${table_location}'
      TBLPROPERTIES (
        'seatunnel.creation.mode' = 'template'
      )
    """
  }
}
```

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
  FakeSource {
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "A", 100] },
      { kind = INSERT, fields = [2, "B", 100] },
      { kind = INSERT, fields = [3, "C", 100] }
    ]
  }
}

sink {
  Hive {
    table_name = "test_hive.test_hive_sink_on_oss"
    metastore_uri = "thrift://master-1-1.c-1009b01725b501f2.cn-wulanchabu.emr.aliyuncs.com:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    hive.hadoop.conf = {
      bucket = "oss://emr-osshdfs.cn-wulanchabu.oss-dls.aliyuncs.com"
    }
  }
}
```

## FAQ

### Why do I see many small files in my Hive table?

Small files are created when job parallelism is high or batches are small. To
reduce small file counts:

- Lower the job `parallelism` setting in the `env` block.
- Run a periodic compaction using Hive's `ALTER TABLE ... CONCATENATE` or a
  Spark merge job.

### Does Hive Sink support schema evolution?

The Hive sink writes the upstream schema into the target table. If columns are
added to the upstream, they will only appear in Hive after the table DDL is
updated; SeaTunnel does not automatically run `ALTER TABLE` on Hive schemas.

### What is the difference between `overwrite` and `data_save_mode`?

`overwrite = true` and `data_save_mode = "DROP_DATA"` behave the same way: the
target directory (table directory for non-partitioned tables, partition
directories for partitioned tables) is deleted before the new data is written.
Use either one, but not both. `data_save_mode = "APPEND_DATA"` (the default)
keeps existing data and appends new rows.

## Changelog

<ChangeLog />
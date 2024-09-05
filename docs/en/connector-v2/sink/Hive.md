# Hive

> Hive sink connector

## Description

Write data to Hive.

## Support Versions

tested hive version :
- 2.3.9
- 3.1.1

# Using Dependency

In order to use this connector, You must ensure your spark/flink cluster already integrated hive.

If you use SeaTunnel Engine, You need put those jar in $SEATUNNEL_HOME/lib/ dir.
- `seatunnel-hadoop3-3.1.4-uber.jar`
- `hive-exec-<hive_version>.jar`
- `libfb303-0.9.3.jar`
- `hive-jdbc-<hive_version>.jar` (if you need `savemode` feature, pass the `hive_jdbc_url` parameter)

## Key Features

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
  - [x] none (default)
  - [x] lzo
  - [x] snappy
  - [x] lz4
  - [x] gzip
  - [x] brotli
  - [x] zstd

## Data Type Mapping

| Hive Data Type | SeaTunnel Data Type |
|----------------|---------------------|
| tinyint        | byte                |
| smallint       | short               |
| int            | int                 |
| bigint         | long                |
| float          | float               |
| double         | double              |
| decimal        | decimal             |
| timestamp      | local_date_time     |
| date           | local_date          |
| string         | string              |
| varchar        | string              |
| boolean        | boolean             |
| binary         | byte array          |
| arrays         | array               |
| maps           | map                 |
| structs        | seatunnel row       |
| char           | not supported       |
| interval       | not supported       |
| union          | not supported       |

## Sink Options

|             Name              |  Type   |     Required     |        Default value         |                                                                                                                                                           Description                                                                                                                                                           |
|-------------------------------|---------|------------------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_name                    | string  | yes              | -                            | Target Hive table name eg: db1.table1, and if the source is multiple mode, you can use `${database_name}.${table_name}` to generate the table name, it will replace the `${database_name}` and `${table_name}` with the value of the CatalogTable generate from the source. The demo config can found in `Multiple Table` part. |
| metastore_uri                 | string  | yes              | -                            | Hive metastore uri                                                                                                                                                                                                                                                                                                              |
| hive_jdbc_url                 | string  | no               | -                            | Hive meta server jdbc url, "jdbc:hive2://127.0.0.1:10000/default" or `jdbc:hive2://127.0.0.1:10000/default;user=<user>;password=<password>` if you use LDAP.                                                                                                                                                                    |
| schema_save_mode              | Enum    | no               | CREATE_SCHEMA_WHEN_NOT_EXIST | the schema save mode, please refer to `schema_save_mode` below                                                                                                                                                                                                                                                                  |
| data_save_mode                | Enum    | no               | APPEND_DATA                  | the data save mode, please refer to `data_save_mode` below                                                                                                                                                                                                                                                                      |
| save_mode_create_template     | string  | yes in condition | see below                    | see below                                                                                                                                                                                                                                                                                                                       |
| save_mode_partition_keys      | list    | yes in condition | see below                    | see below                                                                                                                                                                                                                                                                                                                       |
| compress_codec                | string  | no               | none                         | The compress codec of files                                                                                                                                                                                                                                                                                                     |
| hdfs_site_path                | string  | no               | -                            | The path of `hdfs-site.xml`, used to load ha configuration of namenodes                                                                                                                                                                                                                                                         |
| hive_site_path                | string  | no               | -                            | The path of `hive-site.xml`, used to authentication hive metastore                                                                                                                                                                                                                                                              |
| hive.hadoop.conf              | Map     | no               | -                            | Properties in hadoop conf('core-site.xml', 'hdfs-site.xml', 'hive-site.xml')                                                                                                                                                                                                                                                    |
| hive.hadoop.conf-path         | string  | no               | -                            | The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files                                                                                                                                                                                                                                      |
| krb5_path                     | string  | no               | /etc/krb5.conf               | The path of `krb5.conf`, used to authentication kerberos                                                                                                                                                                                                                                                                        |
| kerberos_principal            | string  | no               | -                            | The principal of kerberos                                                                                                                                                                                                                                                                                                       |
| kerberos_keytab_path          | string  | no               | -                            | The keytab path of kerberos                                                                                                                                                                                                                                                                                                     |
| abort_drop_partition_metadata | boolean | no               | true                         | Flag to decide whether to drop partition metadata from Hive Metastore during an abort operation. Note: this only affects the metadata in the metastore, the data in the partition will always be deleted(data generated during the synchronization process).                                                                    |
| common-options                |         | no               | -                            | Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details                                                                                                                                                                                                                             |


### About Save Mode Feature (added in version x.x.x)

If the table is not exist in hive, or the table struct is not right, we can use this feature to create, re-create table with upstream table's schema before data synchronous.

When you need this feature, you need addiction add `hive_jdbc_url` parameter. we use hive jdbc to hive jdbc to create, re-create table. (If you don't need this feature, just ignore this parameter.)

Here are some related parameter about this feature :

#### schema_save_mode [Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is existed       
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is existed        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist

#### data_save_mode[Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`APPEND_DATA`：Preserve database structure, preserve data  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported. this will run query `select * from table limit 1` to check whether data exist. so it maybe very slow if the table is big.

#### save_mode_create_template

Required when schema_save_mode are `RECREATE_SCHEMA` or `CREATE_SCHEMA_WHEN_NOT_EXIST`  
No default value, you need config it manually.  
We use templates to automatically create Hive tables,  
which will create corresponding table creation statements based on the type of upstream data and schema type,

You can use the following placeholders

- database: Used to get the database in the upstream schema
- table_name: Used to get the table name in the upstream schema
- rowtype_fields: Used to get all the fields in the upstream schema, we will automatically map to the field description of Hive

example template:

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (
${rowtype_fields}
)
partitioned by (col_name col_type)        -- the partition col must exist in upstream, and you need add it to `save_mode_partition_keys`
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location '/tmp/hive/warehouse/default/${database}/${table_name}'
```

**Note:**
1. if you set `partition key`, you need also put it in `save_mode_partition_keys`
2. all the hard code config will apply to all tables, so if you want partition by `colA`, please make sure all upstream table has `colA`

#### save_mode_partition_keys

only required when `save_mode_create_template` has partition definition.  
as we know in hive ddl, partition col is not in column list, so we need this parameter to remove from column lists.

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: lzo none
- json: lzo none
- csv: lzo none
- orc: lzo snappy lz4 zlib none
- parquet: lzo snappy lz4 gzip brotli zstd none

Tips: excel type does not support any compression format

## Config Example

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://127.0.0.1:9083"
  }

```

### Hive Savemode Function

```hacon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 100
    result_table_name = "fake"
    parallelism = 1
    int.template = [18]
    string.template = ["zhangsan"]
    schema = {
      fields {
        name = "string"
        age = "int"
        score = "double"
        c_date = "date"
      }
    }
  }
}

sink {
  Hive {
    source_table_name = "fake"
    table_name = "default.hive_jdbc_example1"
    hive_jdbc_url = "jdbc:hive2://127.0.0.1:10000/default"
    metastore_uri = "thrift://127.0.0.1:9083"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    save_mode_create_template = """
    create table default.${table_name} (
     ${rowtype_fields}
    )
    partitioned by(name string, age int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    location '/tmp/hive/warehouse/${database}/${table_name}'
    """
    save_mode_partition_keys = [name, age]
  }
}
```

in this config, we auto create table `default.hive_jdbc_example1` if table not exist.  
and the ddl will be :

```sql
create table default.hive_jdbc_example1 (
`score` DOUBLE  ,
`c_date` DATE  
)
partitioned by(name string, age int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location '/tmp/hive/warehouse/default/hive_jdbc_example1' 
```

### Hive to Hive

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
    metastore_uri = "thrift://127.0.0.1:9083"
  }
}

sink {

  Hive {
    table_name = "test_hive.test_hive_sink_text_simple"
    metastore_uri = "thrift://127.0.0.1:9083"
    hive.hadoop.conf = {
      bucket = "s3a://mybucket"
      fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
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
    metastore_uri = "thrift://127.0.0.1:9083"
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
    metastore_uri = "thrift://127.0.0.1:9083"
    hive.hadoop.conf-path = "/tmp/hadoop"
    hive.hadoop.conf = {
        bucket="oss://emr-osshdfs.cn-wulanchabu.oss-dls.aliyuncs.com"
    }
  }
}
```

### Multiple Table

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
        metastore_uri = "thrift://127.0.0.1:9083"
      },
      {
        table_name = "test_hive.test_2"
        metastore_uri = "thrift://127.0.0.1:9083"
      }
    ]
  }
}

sink {
  Hive {
    table_name = "${database_name}.${table_name}"
    metastore_uri = "thrift://127.0.0.1:9083"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hive Sink Connector

### 2.3.0-beta 2022-10-20

- [Improve] Hive Sink supports automatic partition repair ([3133](https://github.com/apache/seatunnel/pull/3133))

### 2.3.0 2022-12-30

- [BugFix] Fixed the following bugs that failed to write data to files ([3258](https://github.com/apache/seatunnel/pull/3258))
  - When field from upstream is null it will throw NullPointerException
  - Sink columns mapping failed
  - When restore writer from states getting transaction directly failed

### Next version

- [Improve] Support kerberos authentication ([3840](https://github.com/apache/seatunnel/pull/3840))
- [Improve] Added partition_dir_expression validation logic ([3886](https://github.com/apache/seatunnel/pull/3886))


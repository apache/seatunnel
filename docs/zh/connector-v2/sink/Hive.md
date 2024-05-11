# Hive

> Hive 数据接收器

## 支持的版本

已经验证的版本 :
- 2.3.9
- 3.1.1

# 使用依赖

当你使用Spark/Flink时, 你需要保证已经与Hive进行了集成.  
当你需要使用Zeta引擎时, 你需要将这些依赖放到`$SEATUNNEL_HOME/lib/`目录中.
- `seatunnel-hadoop3-3.1.4-uber.jar`
- `hive-exec-<hive_version>.jar`
- `libfb303-0.9.3.jar`
- `hive-jdbc-<hive_version>.jar` (当设置了`hive_jdbc_url`参数, 需要使用`savemode`功能时)

## 主要特性

- [x] [精准一次](../../concept/connector-v2-features.md)
  我们使用二阶段提交来保证精准一次

- [x] 支持的文件类型

  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
- [x] 支持的压缩方式
  - [x] none (default)
  - [x] lzo
  - [x] snappy
  - [x] lz4
  - [x] gzip
  - [x] brotli
  - [x] zstd

## 数据类型映射

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

## 连接器选项

|              名称               |   类型    |       是否必要       |             默认值              |                                                                                描述                                                                                 |
|-------------------------------|---------|------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_name                    | string  | yes              | -                            | 要写入的表名称 例如: `db1.table1`, 如果source端支持多表, 你可以使用 `${database_name}.${table_name}` 来生成表名,  `${database_name}`和`${table_name}` 这两个值会根据source端生成的信息进行替换. 样例可参考下方`多表写入` |
| metastore_uri                 | string  | yes              | -                            | Hive Metastore的地址                                                                                                                                                 |
| hive_jdbc_url                 | string  | no               | -                            | Hive JDBC的地址 例如: `jdbc:hive2://127.0.0.1:10000/default`, `jdbc:hive2://127.0.0.1:10000/default;user=<user>;password=<password>`                                   |
| schema_save_mode              | Enum    | no               | CREATE_SCHEMA_WHEN_NOT_EXIST | 针对目标侧已有的表结构选择不同的处理方案, 请参考下方的 `schema_save_mode`                                                                                                                   |
| data_save_mode                | Enum    | no               | APPEND_DATA                  | 针对目标侧已存在的数据选择不同的处理方案, 请参考下方的 `data_save_mode`                                                                                                                     |
| save_mode_create_template     | string  | yes in condition | see below                    | 查看下方解释                                                                                                                                                            |
| save_mode_partition_keys      | list    | yes in condition | see below                    | 查看下方解释                                                                                                                                                            |
| compress_codec                | string  | no               | none                         | 文件压缩方式                                                                                                                                                            |
| hdfs_site_path                | string  | no               | -                            | `hdfs-site.xml`文件路径                                                                                                                                               |
| hive_site_path                | string  | no               | -                            | `hive-site.xml`文件路径                                                                                                                                               |
| hive.hadoop.conf              | Map     | no               | -                            | Properties in hadoop conf('core-site.xml', 'hdfs-site.xml', 'hive-site.xml')                                                                                      |
| hive.hadoop.conf-path         | string  | no               | -                            | 也可以指定文件夹, 会从这个文件夹下读取这几个配置('core-site.xml', 'hdfs-site.xml', 'hive-site.xml')                                                                                      |
| krb5_path                     | string  | no               | /etc/krb5.conf               | `krb5.conf`文件路径                                                                                                                                                   |
| kerberos_principal            | string  | no               | -                            | The principal of kerberos                                                                                                                                         |
| kerberos_keytab_path          | string  | no               | -                            | keytab文件路径                                                                                                                                                        |
| abort_drop_partition_metadata | boolean | no               | true                         | 当遇到异常时, 是否回滚Hive MetaStore中的元数据.                                                                                                                                  |
| common-options                |         | no               | -                            | Sink插件常用参数，请参考 [Sink常用选项 ](common-options.md) 了解详情                                                                                                                |

### schema_save_mode [Enum]

在同步任务开始之前, 针对目标侧已有的表结构选择不同的处理方案
选项介绍：  
`RECREATE_SCHEMA` ：无论表是否存在, 都会创建表       
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：当表不存在时创建表       
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当表不存在时, 抛出异常

### data_save_mode[Enum]

在同步任务开始之前, 针对目标侧已存在的数据选择不同的处理方案
选项介绍：  
`APPEND_DATA`：添加数据
`ERROR_WHEN_DATA_EXISTS`：当数据存在将会指向`select * from table limit 1`语句去查询是否存在数据, 当表过大是会导致执行速度慢, 请谨慎使用.

### save_mode_create_template

当`schema_save_mode`是`RECREATE_SCHEMA`, `CREATE_SCHEMA_WHEN_NOT_EXIST`这两个配置时, 需要进行配置.
配置后会根据这个模板来执行建表语句

模板支持这三个变量

- database: 用于获取上游的数据库名称
- table_name: 用于获取上游的表名称
- rowtype_fields: 用于获取上游的字段名称, 并且会自动映射为Hive的字段类型

样例:

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (
${rowtype_fields}
)
partitioned by (col_name col_type)  
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location '/tmp/hive/warehouse/default/${database}/${table_name}'
```

**注意事项:**
1. 当source端为多表时, 创建的多个表都会应用这同一个模板, 所以需要注意这时创建的多个表会用相同的配置, 比如说这里的分隔符, 分区字段(需要保证所有的上游表都有这个分区字段)
2. 当设置了分区字段后, 需要将其也添加到`save_mode_partition_keys`参数中

### save_mode_partition_keys

当`save_mode_create_template`中定义了分区字段时需要设置.

在Hive中, 创建表时分区字段需要单独声明, 并且不能添加在字段中, 所有需要在这里声明然后去除

### compress_codec [string]

文件压缩方式

- txt: lzo none
- json: lzo none
- csv: lzo none
- orc: lzo snappy lz4 zlib none
- parquet: lzo snappy lz4 gzip brotli zstd none

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

这个配置中, 当`default.hive_jdbc_example1`表不存在时将会自动创建  
创表语句为 :

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

当我们有一张这样结构的表:

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

我们需要从这个表读取数据并写入到另外的表中

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

任务配置如下:

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
    }
}
```

## Hive on s3

### Step 1

创建文件夹.

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 2

下载依赖.

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.6.5/hadoop-aws-2.6.5.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### Step 3

复制依赖到相应文件夹中.

```shell
cp /usr/share/aws/emr/emrfs/lib/emrfs-hadoop-assembly-2.60.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/hadoop-common-3.3.6-amzn-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/javax.inject-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/aopalliance-1.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 4

运行.

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
    }
  }
}
```

## Hive on oss

### Step 1

创建文件夹.

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### Step 2

下载依赖.

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### Step 3

复制依赖到相应文件夹中.

```shell
cp -r /opt/apps/JINDOSDK/jindosdk-current/lib/jindo-*.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
rm -f ${SEATUNNEL_HOME}/lib/hadoop-aliyun-*.jar
```

### Step 4

运行

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

### 多表写入

我们有多张source表:

```bash
create table test_1(
)
PARTITIONED BY (xx);

create table test_2(
)
PARTITIONED BY (xx);
...
```

我们需要读取这多张表并写入多张表中, 配置文件如下:

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


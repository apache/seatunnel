import ChangeLog from '../changelog/connector-hive.md';

# Hive

> Hive Sink 连接器

## 描述

将数据写入 Hive。

:::tip 提示

为了使用此连接器，您必须确保您的 Spark/Flink 集群已经集成了 Hive。测试过的 Hive 版本是 2.3.9 和 3.1.3。

如果您使用 SeaTunnel 引擎，您需要将 `seatunnel-hadoop3-3.1.4-uber.jar`、`hive-exec-3.1.3.jar` 和 `libfb303-0.9.3.jar` 放在 `$SEATUNNEL_HOME/lib/` 目录中。
:::

## 关键特性

- [x] [支持多表写入](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)

默认情况下，我们使用 2PC 提交来确保“精确一次”。

- [x] 文件格式
    - [x] 文本
    - [x] CSV
    - [x] Parquet
    - [x] ORC
    - [x] JSON
- [x] 压缩编解码器
    - [x] LZO

## 选项

| 名称                                    | 类型      | 必需 | 默认值            |
|---------------------------------------|---------|----|----------------|
| table_name                            | string  | 是  | -              |
| metastore_uri                         | string  | 是  | -              |
| compress_codec                        | string  | 否  | none           |
| hdfs_site_path                        | string  | 否  | -              |
| hive_site_path                        | string  | 否  | -              |
| hive.hadoop.conf                      | Map     | 否  | -              |
| hive.hadoop.conf-path                 | string  | 否  | -              |
| krb5_path                             | string  | 否  | /etc/krb5.conf |
| kerberos_principal                    | string  | 否  | -              |
| kerberos_keytab_path                  | string  | 否  | -              |
| abort_drop_partition_metadata         | boolean | 否  | false          |
| parquet_avro_write_timestamp_as_int96 | boolean | 否  | false          |
| overwrite                             | boolean | 否  | false          |
| data_save_mode                        | enum    | 否  | APPEND_DATA    |

| schema_save_mode                      | enum    | 否  | CREATE_SCHEMA_WHEN_NOT_EXIST |
| save_mode_create_template             | string  | 否  | -              |
| common-options                        |         | 否  | -              |

### table_name [string]

目标 Hive 表名，例如：`db1.table1`。如果源是多模式，您可以使用 `${database_name}.${table_name}` 来生成表名，它将用源生成的 CatalogTable 的值替换 `${database_name}` 和 `${table_name}`。

### metastore_uri [string]

Hive 元存储 URI

### hdfs_site_path [string]

`hdfs-site.xml` 的路径，用于加载 Namenode 的高可用配置

### hive_site_path [string]

`hive-site.xml` 的路径

### hive.hadoop.conf [map]

Hadoop 配置中的属性（`core-site.xml`、`hdfs-site.xml`、`hive-site.xml`）

### hive.hadoop.conf-path [string]

指定加载 `core-site.xml`、`hdfs-site.xml`、`hive-site.xml` 文件的路径

### krb5_path [string]

`krb5.conf` 的路径，用于 Kerberos 认证

`hive-site.xml` 的路径，用于 Hive 元存储认证

### kerberos_principal [string]

Kerberos 的主体

### kerberos_keytab_path [string]

Kerberos 的 keytab 文件路径

### abort_drop_partition_metadata [boolean]

在中止操作期间是否从 Hive Metastore 中删除分区元数据的标志。注意：这只影响元存储中的元数据，分区中的数据将始终被删除（同步过程中生成的数据）。

### parquet_avro_write_timestamp_as_int96 [boolean]

支持从时间戳写入 Parquet INT96，仅对 parquet 文件有效。

### schema_save_mode [枚举]

### data_save_mode [enum]

在写入数据前，选择如何处理目标端已有数据：

- APPEND_DATA（默认）：保留既有数据并追加写入
- DROP_DATA：与 overwrite=true 等价。在提交前删除目标路径中已有数据（非分区表删除表目录；分区表删除相关分区目录），再写入新数据
- CUSTOM_PROCESSING / ERROR_WHEN_DATA_EXISTS：如无特殊需求，不建议在 Hive sink 下使用

注意：overwrite=true 与 data_save_mode=DROP_DATA 行为等价，二者择一配置即可，勿同时设置。

在开始同步任务之前，针对目标端已存在的表结构选择不同的处理方案。

**默认值**: `CREATE_SCHEMA_WHEN_NOT_EXIST`

选项值：
- `RECREATE_SCHEMA`: 表不存在时会创建，表存在时会删除并重建
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: 表不存在时会创建，表存在时会跳过
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: 表不存在时会报错
- `IGNORE`: 忽略对表的处理



### save_mode_create_template [字符串]

我们使用模板来自动创建 Hive 表，它将根据上游数据类型和模式类型创建相应的建表语句，默认模板可以根据情况进行修改。可用的模板变量：${database}, ${table}, ${rowtype_fields}, ${rowtype_partition_fields}, ${table_location}。

**默认值**: 当未指定时，使用默认的 PARQUET 非分区表模板：
```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
  ${rowtype_fields}
)
STORED AS PARQUET
LOCATION '${table_location}'
```

### 通用选项

Sink 插件的通用参数，请参阅 [Sink Common Options](../sink-common-options.md) 了解详细信息。

## 示例

```bash
  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }
```

### 示例 1

我们有一个源表如下：

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

我们需要从源表读取数据并写入另一个表：

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

作业配置文件可以如下：

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
  # 选择 stdout 输出插件将数据输出到控制台

  Hive {
    table_name = "test_hive.test_hive_sink_text_simple"
    metastore_uri = "thrift://ctyun7:9083"
    hive.hadoop.conf = {
      bucket = "s3a://mybucket"
      fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    }
}
```

### 示例 2：Kerberos

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

描述：

- `hive_site_path`：`hive-site.xml` 文件的路径。
- `kerberos_principal`：Kerberos 认证的主体。
- `kerberos_keytab_path`：Kerberos 认证的 keytab 文件路径。
- `krb5_path`：用于 Kerberos 认证的 `krb5.conf` 文件路径。

运行案例：

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

### 步骤 1

为 EMR 的 Hive 创建 lib 目录。

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### 步骤 2

从 Maven 中心获取 jar 文件到 lib。

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.6.5/hadoop-aws-2.6.5.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### 步骤 3

从您的 EMR 环境中复制 jar 文件到 lib 目录。

```shell
cp /usr/share/aws/emr/emrfs/lib/emrfs-hadoop-assembly-2.60.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/hadoop-common-3.3.6-amzn-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/javax.inject-1.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
cp /usr/share/aws/emr/hadoop-state-pusher/lib/aopalliance-1.0.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### 步骤 4

运行案例。

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

### 步骤 1

为 EMR 的 Hive 创建 lib 目录。

```shell
mkdir -p ${SEATUNNEL_HOME}/plugins/Hive/lib
```

### 步骤 2

从 Maven 中心获取 jar 文件到 lib。

```shell
cd ${SEATUNNEL_HOME}/plugins/Hive/lib
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar
```

### 步骤 3

从您的 EMR 环境中复制 jar 文件到 lib 目录并删除冲突的 jar。

```shell
cp -r /opt/apps/JINDOSDK/jindosdk-current/lib/jindo-*.jar ${SEATUNNEL_HOME}/plugins/Hive/lib
rm -f ${SEATUNNEL_HOME}/lib/hadoop-aliyun-*.jar
```

### 步骤 4

运行案例。

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

### 示例 2

我们有多个源表如下：

```bash
create table test_1(
)
PARTITIONED BY (xx);

create table test_2(
)
PARTITIONED BY (xx);
...
```

我们需要从这些源表读取数据并写入其他表：

作业配置文件可以如下：

```
env {
  # 您可以在此处设置 Flink 配置
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
  # 选择 stdout 输出插件将数据输出到控制台
  Hive {
    table_name = "${database_name}.${table_name}"
    metastore_uri = "thrift://ctyun7:9083"
  }
}
```

## 自动建表示例

### 示例 1：基础自动建表

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
        fields = [1, "张三", "工程部", 75000.50, "2022-01-15"]
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
        department string COMMENT '部门分区'
      )
      STORED AS PARQUET
      LOCATION '${table_location}'
    """
  }
}
```
## 变更日志

<ChangeLog />
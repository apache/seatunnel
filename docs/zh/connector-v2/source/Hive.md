# Hive

> Hive 源连接器

## 描述

从 Hive 读取数据。

:::tip 提示

为了使用此连接器，您必须确保您的 Spark/Flink 集群已经集成了 Hive。测试过的 Hive 版本是 2.3.9 和 3.1.3。

如果您使用 SeaTunnel 引擎，您需要将 `seatunnel-hadoop3-3.1.4-uber.jar`、`hive-exec-3.1.3.jar` 和 `libfb303-0.9.3.jar` 放在 `$SEATUNNEL_HOME/lib/` 目录中。
:::

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)

在 `pollNext` 调用中读取分片中的所有数据。读取的分片将保存在快照中。

- [x] [schema 投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的分片](../../concept/connector-v2-features.md)
- [x] 文件格式
    - [x] 文本
    - [x] CSV
    - [x] Parquet
    - [x] ORC
    - [x] JSON

## 选项

|         名称          |  类型  | 必需 | 默认值  |
|-----------------------|--------|------|---------|
| table_name            | string | 是   | -       |
| metastore_uri         | string | 是   | -       |
| krb5_path             | string | 否   | /etc/krb5.conf |
| kerberos_principal    | string | 否   | -       |
| kerberos_keytab_path  | string | 否   | -       |
| hdfs_site_path        | string | 否   | -       |
| hive_site_path        | string | 否   | -       |
| hive.hadoop.conf      | Map    | 否   | -       |
| hive.hadoop.conf-path | string | 否   | -       |
| read_partitions       | list   | 否   | -       |
| read_columns          | list   | 否   | -       |
| compress_codec        | string | 否   | none    |
| common-options        |        | 否   | -       |

### table_name [string]

目标 Hive 表名，例如：`db1.table1`

### metastore_uri [string]

Hive 元存储 URI

### hdfs_site_path [string]

`hdfs-site.xml` 的路径，用于加载 Namenode 的高可用配置

### hive.hadoop.conf [map]

Hadoop 配置中的属性（`core-site.xml`、`hdfs-site.xml`、`hive-site.xml`）

### hive.hadoop.conf-path [string]

指定加载 `core-site.xml`、`hdfs-site.xml`、`hive-site.xml` 文件的路径

### read_partitions [list]

用户希望从 Hive 表中读取的目标分区，如果用户未设置此参数，将读取 Hive 表中的所有数据。

**提示：分区列表中的每个分区应具有相同的目录层级。例如，一个 Hive 表有两个分区：`par1` 和 `par2`，如果用户设置如下：**
**`read_partitions = [par1=xxx, par1=yyy/par2=zzz]`，这是不合法的**

### krb5_path [string]

`krb5.conf` 的路径，用于 Kerberos 认证

### kerberos_principal [string]

Kerberos 认证的主体

### kerberos_keytab_path [string]

Kerberos 认证的 keytab 文件路径

### read_columns [list]

数据源的读取列列表，用户可以使用它来实现字段投影。

### compress_codec [string]

文件的压缩编解码器，支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  自动识别压缩类型，无需额外设置。

### 通用选项

源插件的通用参数，请参阅 [Source Common Options](../source-common-options.md) 了解详细信息。

## 示例

### 示例 1：单表

```bash
  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }
```

### 示例 2：多表
> 注意：Hive 是结构化数据源，应使用 `table_list`，`tables_configs` 将在未来移除。

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

### 示例 3：Kerberos

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

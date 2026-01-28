import ChangeLog from '../changelog/connector-jdbc.md';

# Kingbase

> JDBC Kingbase 源连接器

## 支持连接器版本

- 8.6

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

通过 JDBC 读取外部数据源数据。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| Kingbase | 8.6 | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [下载](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录<br/>
> 例如：cp kingbase8-8.6.0.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

| Kingbase 数据类型 | SeaTunnel 数据类型 |
|------------------|------------------|
| BOOL | BOOLEAN |
| INT2 | SHORT |
| SMALLSERIAL <br/>SERIAL <br/>INT4 | INT |
| INT8 <br/>BIGSERIAL | BIGINT |
| FLOAT4 | FLOAT |
| FLOAT8 | DOUBLE |
| NUMERIC | DECIMAL |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT | STRING |
| TIMESTAMP | LOCALDATETIME |
| TIME | LOCALTIME |
| DATE | LOCALDATE |
| 其他数据类型 | 暂不支持 |

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | JDBC 连接的 URL。参考示例：jdbc:kingbase8://localhost:54321/test |
| driver | String | 是 | - | 用于连接到远程数据源的 jdbc 类名，应为 `com.kingbase8.Driver`。 |
| username | String | 否 | - | 连接实例用户名 |
| password | String | 否 | - | 连接实例密码 |
| query | String | 是 | - | 查询语句 |
| connection_check_timeout_sec | Int | 否 | 30 | 等待用于验证连接的数据库操作完成的时间（秒） |
| partition_column | String | 否 | - | 用于并行性分割的列名，仅支持数值类型列和字符串类型列。 |
| partition_lower_bound | BigDecimal | 否 | - | partition_column 的最小值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最小值。 |
| partition_upper_bound | BigDecimal | 否 | - | partition_column 的最大值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最大值。 |
| partition_num | Int | 否 | job parallelism | 分割数量，仅支持正整数。默认值是任务并行度。 |
| fetch_size | Int | 否 | 0 | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，以通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。 |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。 |

### 提示

> 如果未设置 partition_column，它将以单并发运行，如果设置了 partition_column，它将根据任务的并发度并行执行。

## 任务示例

### 简单

```
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
  }
}

transform {
    # 如果您想了解有关如何配置 seatunnel 的更多信息并查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```

### 并行

> 使用您配置的分片字段和分片数据并行读取查询表。如果您想读取整个表，可以这样做

```
source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
    # 并行分片读取字段
    partition_column = "id"
    # 分片数量
    partition_num = 10
  }
}
```

### 并行边界

> 根据您配置的上下边界读取数据源更高效

```
source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
    partition_column = "id"
    partition_num = 10
    # 读取开始边界
    partition_lower_bound = 1
    # 读取结束边界
    partition_upper_bound = 500
  }
}
```

## 变更日志

<ChangeLog />


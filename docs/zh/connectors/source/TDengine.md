import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine 源端连接器

## 支持的引擎

> SeaTunnel Zeta<br/>

## 描述

从 TDengine 超级表读取数据。

该 Source 以批处理方式按时间范围读取一个超级表的数据。可以读取该超级表下的所有子表，也可以只读取指定子表；同时支持只读取部分列。

每个 source 分片会读取一个 TDengine 子表。输出字段会自动把 `subtable_name` 放在第一列，用来保留原始子表名；如果下游继续写入 TDengine，TDengine sink 也会使用这一列作为目标子表名。

:::tip

连接器使用 TDengine 的 REST 接口（`jdbc:TAOS-RS://...`），请确保服务器已开启 REST 服务。当前已测试 TDengine 3.x。

:::

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流式](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 源端配置项

| 名称           | 类型   | 必填 | 默认值 | 说明                                                                                                                                                                                       |
|----------------|--------|------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url            | String | 是   | -      | TDengine REST JDBC 连接地址，例如 `jdbc:TAOS-RS://localhost:6041/`。                                                                                                                       |
| username       | String | 是   | -      | 连接 TDengine 使用的用户名。                                                                                                                                                              |
| password       | String | 是   | -      | 连接 TDengine 使用的密码。                                                                                                                                                                  |
| database       | String | 是   | -      | TDengine 数据库名称。                                                                                                                                                                       |
| stable         | String | 是   | -      | 要读取的 TDengine 超级表名称。                                                                                                                                                              |
| sub_tables     | List   | 否   | -      | 要读取的子表名称列表。不配置时读取该超级表下的所有子表；配置后只读取列表中的子表。                                                                                                          |
| lower_bound    | String | 是   | -      | 查询时间范围的下界（包含）。连接器会为每个子表查询加上 `timestamp_column >= lower_bound`。请使用 TDengine 可识别的时间字符串，例如 `2018-10-03 14:38:05.000`。                              |
| upper_bound    | String | 是   | -      | 查询时间范围的上界（不包含）。连接器会为每个子表查询加上 `timestamp_column < upper_bound`。请使用 TDengine 可识别的时间字符串，例如 `2018-10-03 14:38:16.801`。                          |
| read_columns   | List   | 否   | -      | 要读取的列名列表。不配置时读取所有列。读取超级表时，请将 TAGS 字段放在列表末尾；不要包含 `subtable_name`，连接器会自动把它作为第一列输出。                                                |
| common-options |        | 否   | -      | 源端通用参数，请参考 [源端通用选项](../common-options/source-common-options.md) 获取更多细节。                                                                                                |

:::tip

`lower_bound` 和 `upper_bound` 共同决定查询的时间窗口。每个并行 reader 都会在自己的子表范围内扫描该时间窗口内的数据，因此并行度通常对应被读取的子表数量。请确保时间范围覆盖需要读取的全部数据。

:::

### url [String]

TDengine REST JDBC 连接地址，例如：

```
jdbc:TAOS-RS://localhost:6041/
```

### username [String]

连接 TDengine 使用的用户名。

### password [String]

连接 TDengine 使用的密码。

### database [String]

TDengine 数据库名称。

### stable [String]

要读取的 TDengine 超级表名称。每个 source 分片会读取该超级表下的一个子表。

### sub_tables [List]

要读取的子表名称列表。不配置时读取该超级表下的所有子表；配置后只读取列表中的子表。

### lower_bound [String]

查询时间范围的下界（包含）。连接器会为每个子表查询加上
`timestamp_column >= lower_bound`。请使用 TDengine 可识别的时间字符串，
例如 `2018-10-03 14:38:05.000`。

### upper_bound [String]

查询时间范围的上界（不包含）。连接器会为每个子表查询加上
`timestamp_column < upper_bound`。请使用 TDengine 可识别的时间字符串，
例如 `2018-10-03 14:38:16.801`。

### read_columns [List]

要读取的列名列表。不配置时读取所有列。读取超级表时，请将 TAGS 字段放在列表
末尾；不要包含 `subtable_name`，连接器会自动把它作为第一列输出。

`read_columns` 的顺序决定 `subtable_name` 之后的输出字段顺序。如果结果继续
写入 TDengine Sink，请保持普通列在前、TAGS 字段在后，这样 Sink 才能正确区分
普通列和 TAGS 值。

### 通用选项

源端通用参数，请参考
[源端通用选项](../common-options/source-common-options.md) 获取更多细节。

## 任务示例

### 按时间范围读取所有子表

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power"
    stable = "meters"
    lower_bound = "2018-10-03 14:38:05.000"
    upper_bound = "2018-10-03 14:38:16.801"
    plugin_output = "tdengine_result"
  }
}

sink {
  Console {}
}
```

### 读取指定子表和指定列

```hocon
source {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power"
    stable = "meters"
    lower_bound = "2018-10-03 14:38:05.000"
    upper_bound = "2018-10-03 14:38:16.801"
    sub_tables = ["d1001", "d1002"]
    read_columns = ["ts", "current", "voltage", "phase", "off", "nc", "location", "groupid"]
  }
}
```

### 从 TDengine 读取并写入 TDengine

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  TDengine {
    url = "jdbc:TAOS-RS://tdengine-src:6041/"
    username = "root"
    password = "taosdata"
    database = "power"
    stable = "meters"
    lower_bound = "2018-10-03 14:38:05.000"
    upper_bound = "2018-10-03 14:38:16.801"
    plugin_output = "tdengine_result"
  }
}

sink {
  TDengine {
    url = "jdbc:TAOS-RS://tdengine-sink:6041/"
    username = "root"
    password = "taosdata"
    database = "power2"
    stable = "meters2"
    timezone = "UTC"
  }
}
```

## 变更日志

<ChangeLog />
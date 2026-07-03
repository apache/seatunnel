import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine 源端连接器

## 描述

从 TDengine 超级表读取数据。

该 Source 以批处理方式按时间范围读取一个超级表的数据。可以读取该超级表下的所有子表，也可以只读取指定子表；同时支持只读取部分列。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流式](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 配置项

| 名称           | 类型   | 必填 | 默认值         |
|----------------|--------|------|----------------|
| url            | string | 是   | -              |
| username       | string | 是   | -              |
| password       | string | 是   | -              |
| database       | string | 是   | -              |
| stable         | string | 是   | -              |
| sub_tables     | list   | 否   | -              |
| lower_bound    | string | 是   | -              |
| upper_bound    | string | 是   | -              |
| read_columns   | list   | 否   | -              |

### url [string]

TDengine REST JDBC 连接地址。

例如：

```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

连接 TDengine 使用的用户名。

### password [string]

连接 TDengine 使用的密码。

### database [string]

TDengine 数据库名称。

### stable [string]

TDengine 超级表名称。

### sub_tables [list]

TDengine 子表名称列表。不配置时读取该超级表下的所有子表；配置后只读取指定子表。

### lower_bound [string]

查询时间范围的下界，使用 TDengine 可识别的时间字符串，例如 `2018-10-03 14:38:05.000`。

### upper_bound [string]

查询时间范围的上界，使用 TDengine 可识别的时间字符串，例如 `2018-10-03 14:38:16.801`。

### read_columns [list]

要读取的列名列表。不配置时读取所有列。读取超级表时，请将 TAGS 字段放在列表末尾。

## 示例

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

## 变更日志

<ChangeLog />

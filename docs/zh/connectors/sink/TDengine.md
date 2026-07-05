import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine 数据接收器

## 描述

用于将数据写入 TDengine。

运行 SeaTunnel 任务前，需要先创建目标数据库和超级表。该 Sink 支持单表写入，也支持在 `stable` 中使用 `${table_name}` 这类占位符完成多表写入。

输入数据需要符合 TDengine 超级表写入结构：第一列是目标子表名，中间是普通列，最后几列是 TAGS 值。连接器会读取目标超级表元数据来判断末尾有几列 TAGS。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           | 类型   | 是否必传 | 默认值 |
|----------------|--------|----------|--------|
| url            | string | 是       | -      |
| username       | string | 是       | -      |
| password       | string | 是       | -      |
| database       | string | 是       | -      |
| stable         | string | 是       | -      |
| timezone       | string | 否       | UTC    |
| write_columns  | list   | 否       | -      |
| common-options |        | 否       | -      |

### url [string]

TDengine REST JDBC 连接地址。

例如

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

TDengine 超级表名称。多表写入时可以使用占位符，例如 `${table_name}`。

### timezone [string]

TDengine 服务端时区，用于时间戳转换，默认值为 `UTC`。

### write_columns [list]

要写入 TDengine 的普通列名列表。不配置时，TDengine 会按目标超级表的列顺序写入。这里不要包含第一列子表名，也不要包含 TAGS 字段；连接器会自动从输入数据末尾取出 TAGS 值。

### 通用选项

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。
多表写入时，可以配合通用参数中的 `multi_table_sink_replica` 使用。

## 示例

### 写入单个超级表

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

sink {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power2"
    stable = "meters2"
    timezone = "UTC"
    write_columns = ["ts", "voltage", "current", "power"]
  }
}
```

### 多表写入匹配的超级表

```hocon
source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema = {
          table = "meters3"
          fields {
            device_id = "string"
            event_time = "timestamp"
            metric1 = "float"
            metric2 = "int"
            metric3 = "float"
            status_flag = "boolean"
            notes = "string"
            location_tag = "string"
            group_tag = "int"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = ["d2001", "2023-04-22T14:38:05", 10.3, 219, 0.31, true, "nc", "California.SanFrancisco", 2]
          }
        ]
      },
      {
        schema = {
          table = "meters4"
          fields {
            device_id = "string"
            event_time = "timestamp"
            metric1 = "float"
            metric2 = "int"
            metric3 = "float"
            status_flag = "boolean"
            notes = "string"
            location_tag = "string"
            group_tag = "int"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = ["d1005", "2023-04-22T14:38:05", 110.3, 219, 0.31, true, "nc", "California.SanFrancisco", 2]
          }
        ]
      }
    ]
  }
}

sink {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power2"
    stable = "${table_name}"
    timezone = "UTC"
  }
}
```

## 变更日志

<ChangeLog />

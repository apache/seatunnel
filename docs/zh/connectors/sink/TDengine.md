import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于将数据写入 TDengine。

运行 SeaTunnel 任务前，需要先创建目标数据库和超级表。该 Sink 支持单表写入，也支持在 `stable` 中使用 `${table_name}` 这类占位符完成多表写入。

输入数据需要符合 TDengine 超级表写入结构：第一列是目标子表名，中间是普通列，最后几列是 TAGS 值。连接器会读取目标超级表元数据来判断末尾有几列 TAGS。

例如，目标超级表有 2 个 TAGS 字段时，输入数据最后 2 列会作为 TAGS 值，第一列会作为子表名。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           | 类型   | 是否必传 | 默认值 | 说明 |
|----------------|--------|----------|--------|------|
| url            | String | 是       | -      | TDengine REST JDBC 连接地址，例如 `jdbc:TAOS-RS://localhost:6041/`。 |
| username       | String | 是       | -      | 连接 TDengine 使用的用户名。 |
| password       | String | 是       | -      | 连接 TDengine 使用的密码。 |
| database       | String | 是       | -      | TDengine 数据库名称。 |
| stable         | String | 是       | -      | TDengine 超级表名称。多表写入时可以使用占位符，例如 `${table_name}`。 |
| timezone       | String | 否       | UTC    | TDengine 服务端时区，用于时间戳转换。 |
| write_columns  | List   | 否       | -      | 要写入 TDengine 的普通列名列表。不配置时按目标超级表的列顺序写入；不要包含子表名列或 TAGS 字段。 |
| common-options |        | 否       | -      | Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。 |

### url [String]

TDengine REST JDBC 连接地址。

例如：

```
jdbc:TAOS-RS://localhost:6041/
```

### username [String]

连接 TDengine 使用的用户名。

### password [String]

连接 TDengine 使用的密码。

### database [String]

TDengine 数据库名称，数据库必须已经在服务端存在。

### stable [String]

TDengine 超级表名称。该值会被 Sink Writer 原样使用，TDengine 连接器本身不会执行占位符替换，因此 `${table_name}` 等占位符不会在运行时被替换。在多表写入场景下，上游 SeaTunnel 框架（`TablePlaceholderProcessor`）可能在任务初始化阶段根据上游 `CatalogTable` 的标识替换一次 `stable`，但这取决于上游框架的接线，并不是 TDengine 连接器自身的能力。

### timezone [String]

TDengine 服务端时区，用于时间戳转换，默认值为 `UTC`。如果服务端不是 UTC 时区，请把该项设置为与服务端一致的时区。

### write_columns [List]

要写入 TDengine 的普通列名列表。不配置时，TDengine 会按目标超级表的列顺序写入。这里不要包含第一列子表名，也不要包含 TAGS 字段；连接器会自动从输入数据末尾取出 TAGS 值。

### 通用选项

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。
多表写入时，可以配合通用参数中的 `multi_table_sink_replica` 使用。

## 输入数据格式

连接器要求每行输入数据符合超级表写入结构：

1. 第一列为目标子表名（字符串）。若该子表不存在，Sink 会按目标超级表的 schema 自动创建。
2. 接下来的列为 `write_columns` 中声明的普通列（未配置时使用目标超级表的列顺序）。
3. 末尾几列为 TAGS 值。TAGS 字段的数量会从目标超级表的元数据中读取。

例如，目标超级表有 2 个 TAGS 字段时，输入行最后 2 列会作为 TAGS 值，第一列会作为子表名。

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

这里的 `${table_name}` 会被 TDengine Sink Writer 当作普通字符串原样使用（连接器不会按行替换它），因此本示例只有在任务初始化阶段由上游框架依据 `CatalogTable` 的标识替换 `stable` 时才能生效。目标超级表必须已经存在并具有匹配的 TAGS 列。

## 变更日志

<ChangeLog />

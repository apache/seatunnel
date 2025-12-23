import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Sink Connector

`Sink: HugeGraph`

## 描述

HugeGraph sink连接器允许您将数据从SeaTunnel写入Apache HugeGraph，这是一个快速且可扩展的图数据库。

该连接器支持将数据作为顶点或边写入，提供了从关系数据模型到图结构的灵活映射。它专为高性能数据加载而设计。

## 特性

- **批量写入**: 数据分批写入，以实现高吞吐量。
- **灵活映射**: 支持将源字段灵活映射到顶点/边属性。
- **顶点和边写入**: 可以将数据作为顶点或边写入。
- **自动创建Schema**: 如果不存在，可以自动创建图Schema元素（属性键、顶点标签、边标签）。

## 配置选项

| 名称                | 类型    | 是否必须 | 默认值 | 描述                                                                   |
| ------------------- | ------- | -------- | ------ | ---------------------------------------------------------------------- |
| `host`              | String  | 是       | -      | HugeGraph服务器的主机。                                                |
| `port`              | Integer | 是       | -      | HugeGraph服务器的端口。                                                |
| `graph_name`        | String  | 是       | -      | 要写入的图的名称。                                                     |
| `graph_space`       | String  | 是       | -      | 要操作的图的图空间。                                                   |
| `username`          | String  | 否       | -      | 用于HugeGraph身份验证的用户名。                                        |
| `password`          | String  | 否       | -      | 用于HugeGraph身份验证的密码。                                          |
| `batch_size`        | Integer | 否       | 500    | 在单批次写入HugeGraph之前缓冲的记录数。                                |
| `batch_interval_ms` | Integer | 否       | 5000   | 刷新批次前等待的最大时间（毫秒）。                                     |
| `max_retries`       | Integer | 否       | 3      | 重试失败写入操作的最大次数。                                           |
| `retry_backoff_ms`  | Integer | 否       | 5000   | 重试之间的退避时间（毫秒）。                                           |

## Sink选项

| 名称               | 类型   | 是否必须 | 默认值 | 描述                                                                 |
| ------------------ | ------ | -------- | ------ | -------------------------------------------------------------------- |
| `schema_config`    | Object | 是       | -      | 将输入数据映射到HugeGraph的Schema（顶点或边）的配置。                |
| `selected_fields`  | List   | 否       | -      | 要从输入数据中选择的字段列表。如果未指定，将使用所有字段。           |
| `ignored_fields`   | List   | 否       | -      | 要从输入数据中忽略的字段列表。与`selected_fields`互斥。              |

### Schema配置 (`schema_config`)

`schema_config`列表中的每个对象都定义了从源数据到HugeGraph中特定顶点或边标签的映射。

| 名称               | 类型                | 是否必须 | 默认值  | 描述                                                         |
| ------------------ | ------------------- | -------- | ------- |------------------------------------------------------------|
| `type`             | String              | 是       | -       | 要映射到的图元素的类型。必须是`VERTEX`或`EDGE`。                            |
| `label`            | String              | 是       | -       | HugeGraph中顶点或边的标签。                                         |
| `properties`       | `List<String>`        | 否       | -       | 顶点或边的源字段名称列表。                                              |
| `ttl`              | Long                | 否       | -       | 顶点或边的生存时间（秒）。                                              |
| `ttlStartTime`     | String              | 否       | -       | TTL的开始时间。                                                  |
| `enableLabelIndex` | Boolean             | 否       | `false` | 是否为此标签启用标签索引。                                              |
| `userdata`         | `Map<String, Object>` | 否       | -       | 与标签关联的用户定义数据。                                              |
| `idStrategy`       | String              | 对于顶点 | -       | 顶点的ID生成策略。支持的值：`PRIMARY_KEY`、`CUSTOMIZE_UUID`、`AUTOMATIC`。 |
| `idFields`         | `List<String>`        | 对于顶点 | -       | 用于生成顶点ID的源字段名称列表。                                          |
| `sourceConfig`     | Object              | 对于边   | -       | 定义边的源顶点映射的对象。请参阅下面的`Source/Target Config`。                 |
| `targetConfig`     | Object              | 对于边   | -       | 定义边的目标顶点映射的对象。请参阅下面的`Source/Target Config`。                |
| `frequency`        | String              | 对于边   | -       | 边的频率，例如`SINGLE`、`MULTIPLE`。                                |
| `mapping`          | Object              | 否       | -       | 定义高级字段和值映射的对象。请参阅下面的`Mapping Config`。                      |

### Source/Target配置 (`sourceConfig` 和 `targetConfig`)

此对象在`EDGE` Schema中使用，用于定义如何识别源顶点和目标顶点。

| 名称       | 类型         | 是否必须 | 默认值 | 描述                                                                                                                                         |
| ---------- | ------------ | -------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `label`    | String       | 是       | -      | 源或目标顶点的标签。                                                                                                                         |
| `idFields` | `List<String>` | 是       | -      | 用于构造源/目标顶点ID的输入行中的源字段名称列表。这些值将被连接起来形成顶点ID。                                                              |

### Mapping配置 (`mapping`)

此对象提供对字段和值如何映射到属性的高级控制。

| 名称              | 类型                | 是否必须 | 默认值       | 描述                                                                                                                                                                      |
| ----------------- | ------------------ | -------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fieldMapping`    | `Map<String, String>` | 否       | -            | 一个映射，其中键是源字段名，值是HugeGraph中的目标属性名。如果未指定，则使用源字段名作为目标属性名。                                                                         |
| `valueMapping`    | `Map<Object, Object>` | 否       | -            | 用于转换特定字段值的映射。键是源的原始值，值是要写入的新值。                                                                                                               |
| `nullableKeys`    | `List<String>`       | 否       | -            | 可以具有null值的属性键列表。                                                                                                                                              |
| `nullValues`      | `List<String>`       | 否       | -            | 应被视为`null`的字符串值列表。任何包含这些值的字段都不会被写入。                                                                                                          |
| `dateFormat`      | String             | 否       | `yyyy-MM-dd` | 用于解析日期字符串的日期格式。                                                                                                                                            |
| `timeZone`        | String             | 否       | `GMT+8`      | 用于日期解析的时区。                                                                                                                                                      |
| `sortKeys`         | `List<String>`       | 对于边   | -            | 用于对具有相同源和目标顶点的边进行排序的属性键列表。                                                                                                                      |

## 使用示例

### 1. 写入顶点

此示例展示了如何从`FakeSource`读取数据并将`person`顶点写入HugeGraph。顶点ID基于`name`字段。

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_input = "fake_source"
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    graph_space = "default"
    selected_fields = ["name", "age"]
    schema_config = {
      type = "VERTEX"
      label = "person"
      idStrategy = "PRIMARY_KEY"
      idFields = ["name"]
      properties = ["name", "age"]
    }
  }
}
```

### 2. 写入边

此示例将一个关系表同步为HugeGraph中的`knows`边。源表包含相互认识的两个人的姓名以及他们相识的年份。

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_input = "fake_source"
    schema = {
      fields = {
        person1_name = "string"
        person2_name = "string"
        since = "int"
      }
    }
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    graph_space = "default"
    schema_config = {
      type = "EDGE"
      label = "knows"
      sourceConfig = {
        label = "person"
        idFields = ["person1_name"]
      }
      targetConfig = {
        label = "person"
        idFields = ["person2_name"]
      }
      properties = ["since"]
      mapping = {
        fieldMapping = {
          person1_name = "name"
          person2_name = "name"
        }
      }
    }
  }
}
```

## Changelog

<ChangeLog />

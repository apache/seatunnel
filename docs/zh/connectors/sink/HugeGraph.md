import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Sink Connector

`Sink: HugeGraph`

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

HugeGraph sink连接器允许您将数据从SeaTunnel写入Apache HugeGraph，这是一个快速且可扩展的图数据库。

该连接器支持将数据作为顶点或边写入，提供了从关系数据模型到图结构的灵活映射。它专为高性能数据加载而设计。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

该连接器可以把输入行写成顶点或边，支持插入、更新、删除，并且会在达到 `batch_size`、checkpoint 或 close 时刷新缓存数据。

:::caution

新的 `mappings` 配置默认使用 `schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST`，写入前会创建缺失的 HugeGraph PropertyKey/VertexLabel/EdgeLabel。Legacy `schema_config` 任务在未显式设置该选项时保留原有的 `ERROR_WHEN_SCHEMA_NOT_EXIST` 行为。

:::

## 配置选项

| 名称                | 类型    | 是否必须 | 默认值 | 描述                                                                   |
| ------------------- | ------- | -------- | ------ | ---------------------------------------------------------------------- |
| `host`              | String  | 是       | -      | HugeGraph服务器的主机。                                                |
| `port`              | Integer | 是       | -      | HugeGraph服务器的端口。                                                |
| `protocol`          | String  | 否       | `http` | 服务协议，支持 `http`、`https`。HTTPS 使用 JVM trust store。             |
| `graph_name`        | String  | 是       | -      | 要写入的图的名称。                                                     |
| `graph_space`       | String  | 否       | `DEFAULT` | 图所属的图空间（graph space）。 |
| `username`          | String  | 否       | -      | 用于HugeGraph身份验证的用户名。                                        |
| `password`          | String  | 否       | -      | 用于HugeGraph身份验证的密码。                                          |
| `batch_size`        | Integer | 否       | 500    | 在单批次写入HugeGraph之前缓冲的记录数。                                |
| `batch_interval_ms` | Integer | 否       | 5000   | 为兼容性保留。在 Zeta 上需要定时刷新时，请在作业 `env` 中配置 `sink.flush.interval`。 |
| `batch_failure_fallback` | Boolean | 否   | true   | 批量写入失败时，降级为逐条写入，使单条“毒药”记录不再拖垮整批。失败记录会记录日志并跳过，其余成功；若整批全部失败（系统性错误）则抛出。设为 `false` 则整批失败。 |
| `max_insert_errors` | Integer | 否       | 500    | 逐条降级（`batch_failure_fallback=true`）累计跳过的失败记录达到该数量后使任务失败，用于约束原本无上限的“毒药”记录静默跳过。设为 `-1` 表示不限。仅在开启 `batch_failure_fallback` 时生效。 |
| `failure_data_path` | String  | 否       | -      | 可选本地目录。设置后，逐条降级跳过的每条记录（映射后的 id、label、属性及服务端错误）会追加写入按子任务区分的文件（`hugegraph-sink-failures-subtask-N.log`）以便离线排查。集群模式下文件写在运行该 sink 子任务的 worker 节点上。 |
| `check_vertex`      | Boolean | 否       | false  | 写入边时服务端是否校验边的源/目标顶点是否存在。为 `false` 时，端点从未写入的边会被写成孤儿边（或触发服务端幻影顶点自动创建）。开启后此类边会被拒绝。 |
| `max_retries`       | Integer | 否       | 3      | 首次请求失败后的重试次数。设置为 `0` 可禁用重试。                       |
| `retry_backoff_ms`  | Integer | 否       | 5000   | 重试的基础退避时间（毫秒），按尝试次数指数增长（`retry_backoff_ms * 2^(attempt-1)`），上限为 `retry_backoff_max_ms`。 |
| `retry_backoff_max_ms` | Integer | 否   | 30000  | 指数退避的上限（毫秒）。                                               |

## Sink选项

| 名称                       | 类型    | 是否必须 | 默认值 | 描述 |
|----------------------------|---------|----------|--------|------|
| `mappings`                 | List    | 是       | -      | 推荐的映射配置。每个条目将输入行映射到一个 HugeGraph 顶点或边标签。 |
| `schema_save_mode`         | Enum    | 否       | `mappings` 为 `CREATE_SCHEMA_WHEN_NOT_EXIST`；legacy 为 `ERROR_WHEN_SCHEMA_NOT_EXIST` | Schema 管理模式。 |
| `data_save_mode`           | Enum    | 否       | `APPEND_DATA` | 写入前如何处理已有数据。`APPEND_DATA` 保留已有数据；`DROP_DATA` 在任务开始时**仅**删除本任务涉及的 label 的数据（先边后点），保留其 schema 以及其他 label 的数据；删除按 label 隔离（某张表的 DROP 不会波及其他表），且在 checkpoint 重启时不会重复执行。 |
| `delete_vertex_with_edges` | Boolean | 否       | `mappings` 为 `false`；legacy 为 `true` | 为 true 时，顶点 DELETE 行会同时删除关联边。 |
| `schema_config`            | Object  | 否       | -      | 已废弃的 legacy 映射对象。请使用 `mappings`。必须配置 `mappings` 或 `schema_config` 之一。 |
| `selected_fields`          | List    | 否       | -      | 已废弃。Legacy `schema_config` 仍会应用；新任务请使用 mapping 内的 `properties`。 |
| `ignored_fields`           | List    | 否       | -      | 已废弃。Legacy `schema_config` 仍会应用；新任务请使用 mapping 内的 `properties`。 |

如果同时配置 `mappings` 和 `schema_config`，connector 会使用 `mappings`，并输出警告说明 `schema_config` 被忽略。

## 定时刷新

定时刷新是仅由 Zeta 支持的引擎级能力。在作业的 `env` 中配置 `sink.flush.interval` 后，即使尚未达到 `batch_size`，HugeGraph Sink 也会写出待处理的记录。Spark 和 Flink 不会注入 `FlushSignal`，因此不会触发这种定时刷新。

```hocon
env {
  sink.flush.interval = 5000
}
```

HugeGraph 定时刷新复用连接器现有的同步批量刷新。失败会直接传递给引擎，而不会被连接器自建后台线程延迟暴露。

### 映射配置 (`mappings`)

每个 `mappings` 条目定义输入行如何映射到一个 HugeGraph 顶点标签或边标签。

| 名称               | 类型                | 是否必须 | 默认值  | 描述 |
|--------------------|---------------------|----------|---------|------|
| `type`             | String              | 是       | -       | 要映射到的图元素类型。必须是 `VERTEX` 或 `EDGE`。 |
| `label`            | String              | 是       | -       | HugeGraph 中顶点或边的标签。 |
| `properties`       | `List<String>`        | 否       | -       | 要写入 HugeGraph 属性的源字段名。为空时会考虑所有输入字段。 |
| `ttl`              | Long                | 否       | -       | 顶点或边的生存时间，单位秒。 |
| `ttlStartTime`     | String              | 否       | -       | TTL 的开始时间。 |
| `enableLabelIndex` | String              | 否       | -       | 随 mapping 配置传入的预留标签索引配置。 |
| `userdata`         | `Map<String, Object>` | 否       | -       | 与标签关联的用户自定义数据。 |
| `idStrategy`       | String              | 对于顶点 | -       | 顶点 ID 生成策略，例如 `PRIMARY_KEY`、`CUSTOMIZE_STRING`、`CUSTOMIZE_NUMBER`、`CUSTOMIZE_UUID` 或 `AUTOMATIC`。 |
| `idFields`         | `List<String>`        | 对于顶点 | -       | 用于生成顶点 ID 的源字段名。当 `idStrategy` 不是 `AUTOMATIC` 时必填。 |
| `sourceConfig`     | Object              | 对于边   | -       | 定义边的源顶点映射。请参阅下面的 `Source/Target Config`。 |
| `targetConfig`     | Object              | 对于边   | -       | 定义边的目标顶点映射。请参阅下面的 `Source/Target Config`。 |
| `frequency`        | String              | 对于边   | -       | 边频率，例如 `SINGLE`、`MULTIPLE`。 |
| `sortKeys`         | `List<String>`        | 对于边   | -       | **输入行中的源字段名**（映射前、即 `fieldMapping` 应用之前的名字），用于区分相同源点和目标点之间的多条边。当 `frequency = MULTIPLE` 时必填。示例：当 `fieldMapping = {event_time: created_at}` 时，应填 `sortKeys = [event_time]`，而不是 `[created_at]`。 |
| `fieldMapping`     | `Map<String, String>` | 否       | -       | 字段映射，key 为源字段名，value 为 HugeGraph 目标属性名。 |
| `valueMapping`     | `Map<String, Map<Object, Object>>` | 否       | -       | 按字段的值转换映射。外层键为源字段名，内层为 `原始值 -> 新值`。按字段隔离可避免一个列的规则影响其他列（如 `gender` 的 M->male 不会改写 `status` 的 M）。 |
| `ignored`          | `List<String>`      | 否       | -       | 从属性中排除的源字段黑名单（仅隐式模式生效）。与 `properties`（充当 selected 白名单）互斥。 |
| `updateStrategies` | `Map<String, String>` | 否       | -       | 写入时按目标属性名指定的属性级合并策略：`OVERRIDE`、`APPEND`、`SUM`、`UNION`、`BIGGER`、`SMALLER` 等。设置后对已存在元素做合并而非覆盖。 |
| `nullableKeys`     | `List<String>`        | 否       | -       | 自动建 label 时允许为 null 的属性键白名单。设置后覆盖下述默认行为（仅这些键可空）。主键、`MULTIPLE` 边的 sortKeys 等 key 属性始终排除。与 `notNullableKeys` 互斥。 |
| `notNullableKeys`  | `List<String>`        | 否       | -       | 与默认可空行为配合使用的反向 opt-out 列表。默认情况下（既未配 `nullableKeys` 也未配 `notNullableKeys`），自动建 label 的所有非 key 属性均可空；在此列出必须为非空的属性。与 `nullableKeys` 互斥。仅影响新建 label。 |
| `nullValues`       | `List<String>`        | 否       | -       | 应被视为 `null` 的字符串值列表。 |
| `dateFormat`       | String              | 否       | `yyyy-MM-dd` | 用于解析日期字符串的日期格式。 |
| `extraDateFormats` | `List<String>`      | 否       | -       | 解析日期字符串时，在 `dateFormat` 之后按顺序尝试的额外日期格式——用于多源汇入、日期格式不一致的场景。 |
| `listFormat`       | Object              | 否       | -       | 原始字符串如何解析为 SET/LIST 属性元素：`startSymbol`（默认 `[`）、`endSymbol`（默认 `]`）、`elemDelimiter`（默认 `,`）、`ignoredElems`。 |
| `unfold`           | Boolean             | 否       | false   | （顶点）把 list 型 CUSTOMIZE id 单元格展开为每个元素一个顶点。仅 INSERT/append。 |
| `unfoldSource`     | Boolean             | 否       | false   | （边）把 list 型源端点 id 单元格展开为多条边（CUSTOMIZE 端点）。仅 INSERT/append。 |
| `unfoldTarget`     | Boolean             | 否       | false   | （边）把 list 型目标端点 id 单元格展开为多条边（与源端笛卡尔积）。仅 INSERT/append。 |
| `timeZone`         | String              | 否       | Worker JVM 默认 | 用于日期解析的时区。省略时使用 Worker JVM 默认时区，与 HugeGraph Source 一致，从而保证 Source→Sink 往返时绝对时间不变。 |

### Legacy Schema配置 (`schema_config`)

`schema_config` 定义一个输入流如何映射到 HugeGraph 中的某个顶点标签或边标签。该配置已废弃，新任务应使用 `mappings`。

| 名称               | 类型                | 是否必须 | 默认值  | 描述                                                         |
| ------------------ | ------------------- | -------- | ------- |------------------------------------------------------------|
| `type`             | String              | 是       | -       | 要映射到的图元素的类型。必须是`VERTEX`或`EDGE`。                            |
| `label`            | String              | 是       | -       | HugeGraph中顶点或边的标签。                                         |
| `tablePath`        | String              | 否       | -       | Schema 配置中携带的预留表路径值。                                   |
| `properties`       | `List<String>`        | 否       | -       | 顶点或边的源字段名称列表。                                              |
| `ttl`              | Long                | 否       | -       | 顶点或边的生存时间（秒）。                                              |
| `ttlStartTime`     | String              | 否       | -       | TTL的开始时间。                                                  |
| `enableLabelIndex` | String              | 否       | -       | 预留的标签索引配置，会随 Schema 配置传入。                              |
| `userdata`         | `Map<String, Object>` | 否       | -       | 与标签关联的用户定义数据。                                              |
| `idStrategy`       | String              | 对于顶点 | -       | 顶点的 ID 生成策略，例如：`PRIMARY_KEY`、`CUSTOMIZE_STRING`、`CUSTOMIZE_NUMBER`、`CUSTOMIZE_UUID`、`AUTOMATIC`。 |
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
| `idFields` | `List<String>` | 是       | -      | 用于构造源/目标顶点ID的输入行中的源字段名称列表。这些值将被连接起来形成顶点ID。对于 HugeGraph → HugeGraph 克隆，可将其设为已携带完整端点 id 的保留列——`sourceConfig` 用 `["~source_id"]`，`targetConfig` 用 `["~target_id"]`——连接器会直接复用该 id（见下方 *从 HugeGraph Source 克隆*）。 |

### 从 HugeGraph Source 克隆（保留列 id 直连）

当输入来自 HugeGraph Source 时，每行都带有保留列，携带已拼好的元素 id（顶点为 `~id`；边的端点为 `~source_id`/`~target_id`）。全保真克隆方式：

- **顶点**（`CUSTOMIZE_STRING`/`CUSTOMIZE_NUMBER`/`CUSTOMIZE_UUID` id）：将 `idStrategy` 设为对应的 `CUSTOMIZE_*`，`idFields = ["~id"]`，原始 id 原样写入。`PRIMARY_KEY` 顶点改用其主键属性列（Source 已输出）；`AUTOMATIC` id 无法保留（目标服务端会重新分配）。
- **边**：设 `sourceConfig.idFields = ["~source_id"]`、`targetConfig.idFields = ["~target_id"]`。端点 id 被直接复用，因此无论端点顶点的 id 策略为何都能克隆。目标端点的顶点 label 必须已存在（连接器不会用保留 id 自动建顶点 label）。

### Mapping配置 (`mapping`)

此对象提供对字段和值如何映射到属性的高级控制。

| 名称              | 类型                | 是否必须 | 默认值       | 描述                                                                                                                                                                      |
| ----------------- | ------------------ | -------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fieldMapping`    | `Map<String, String>` | 否       | -            | 一个映射，其中键是源字段名，值是HugeGraph中的目标属性名。如果未指定，则使用源字段名作为目标属性名。                                                                         |
| `valueMapping`    | `Map<String, Map<Object, Object>>` | 否       | -            | 按字段的值转换映射。外层键为源字段名，内层为 `原始值 -> 新值`。按字段隔离，一个列的替换规则不会影响其他列。                                                                                                               |
| `ignored`          | `List<String>`      | 否       | -       | 从属性中排除的源字段黑名单（仅隐式模式生效）。与 `properties`（充当 selected 白名单）互斥。 |
| `updateStrategies` | `Map<String, String>` | 否       | -       | 写入时按目标属性名指定的属性级合并策略：`OVERRIDE`、`APPEND`、`SUM`、`UNION`、`BIGGER`、`SMALLER` 等。设置后对已存在元素做合并而非覆盖。 |
| `nullableKeys`    | `List<String>`       | 否       | -            | 自动建 label 时允许为 null 的属性键白名单。设置后覆盖默认可空行为。与 `notNullableKeys` 互斥。                                                                              |
| `notNullableKeys` | `List<String>`       | 否       | -            | 与默认可空行为配合的反向 opt-out 列表，在此列出必须为非空的属性。与 `nullableKeys` 互斥。                                                                                    |
| `nullValues`      | `List<String>`       | 否       | -            | 应被视为`null`的字符串值列表。任何包含这些值的字段都不会被写入。                                                                                                          |
| `dateFormat`      | String             | 否       | `yyyy-MM-dd` | 用于解析日期字符串的日期格式。                                                                                                                                            |
| `extraDateFormats`| `List<String>`     | 否       | -            | 解析日期字符串时，在 `dateFormat` 之后按顺序尝试的额外日期格式——用于多源汇入、日期格式不一致的场景。                                                                          |
| `listFormat`      | Object             | 否       | -            | 原始字符串如何解析为 SET/LIST 属性元素：`startSymbol`（默认 `[`）、`endSymbol`（默认 `]`）、`elemDelimiter`（默认 `,`）、`ignoredElems`。                                       |
| `unfold`          | Boolean            | 否       | false        | （顶点）把 list 型 CUSTOMIZE id 单元格展开为每个元素一个顶点。仅 INSERT/append。                                                                                              |
| `unfoldSource`    | Boolean            | 否       | false        | （边）把 list 型源端点 id 单元格展开为多条边（CUSTOMIZE 端点）。仅 INSERT/append。                                                                                            |
| `unfoldTarget`    | Boolean            | 否       | false        | （边）把 list 型目标端点 id 单元格展开为多条边（与源端笛卡尔积）。仅 INSERT/append。                                                                                          |
| `timeZone`        | String             | 否       | Worker JVM 默认 | 用于日期解析的时区。省略时使用 Worker JVM 默认时区。                                                                                                                    |
| `sortKeys`         | `List<String>`       | 对于边   | -            | **输入行中的源字段名**（`fieldMapping` 应用之前），用于区分相同源点和目标点之间的多条边。示例：当 `fieldMapping = {event_time: created_at}` 时，应填 `[event_time]`，而不是 `[created_at]`。                                                                                                                      |

## 支持的数据类型

写入前，连接器会校验 SeaTunnel 行结构和 HugeGraph 中已经存在的 Schema 是否匹配。

| SeaTunnel 类型 | HugeGraph 属性类型 |
|----------------|--------------------|
| `BYTES`        | `BLOB`             |
| `TINYINT`      | `INT`              |
| `SMALLINT`     | `INT`              |
| `INT`          | `INT`              |
| `BIGINT`       | `LONG`             |
| `FLOAT`        | `FLOAT`            |
| `DOUBLE`       | `DOUBLE`           |
| `BOOLEAN`      | `BOOLEAN`          |
| `DATE`         | `DATE`             |
| `TIMESTAMP`    | `DATE`             |
| `ARRAY`        | HugeGraph 中非单值属性，且数组元素类型兼容 |
| `STRING`       | `TEXT`             |
| `DECIMAL`      | `TEXT`             |
| `MAP`          | `TEXT`             |
| `ROW`          | `TEXT`             |
| `TIME`         | `TEXT`             |
| `NULL`         | `TEXT`             |

## 写入行为说明

- 写入顶点时，`idStrategy` 决定如何生成顶点 ID。`PRIMARY_KEY` 会按 HugeGraph 主键格式拼接所有 `idFields`；`CUSTOMIZE_STRING` 在多字段时用 `:` 拼接（并对字段值中的 `:` 做反斜杠转义，避免不同字段组合产生相同 id；单字段时原样使用）；`CUSTOMIZE_NUMBER` 需要一个整数值字段（`1.9` 之类的小数会被拒绝，不会被静默截断）；`CUSTOMIZE_UUID` 需要一个 UUID 字段。
- 写入边时，连接器会从 HugeGraph 中已有的源顶点标签和目标顶点标签读取 ID 策略。`sourceConfig.idFields` 和 `targetConfig.idFields` 必须能还原对应顶点 ID。
- `INSERT` 会写入新的顶点或边，`UPDATE_AFTER` 会更新已有图元素，`DELETE` 会删除图元素。删除行只需要包含能生成图元素 ID 的字段。
- `AUTOMATIC` 顶点 ID 仅支持 INSERT；UPDATE 和 DELETE 必须使用可还原 ID 的策略。
- Sink 提供 at-least-once 语义。使用 `AUTOMATIC` ID 时，重试或 checkpoint 恢复后的 INSERT 重放可能产生重复顶点。
- 边批次使用所配置的 `check_vertex`（默认 `false`）。默认情况下顶点与边可能乱序写入，所有批次完成后图达到最终一致状态；设为 `check_vertex=true` 则服务端会拒绝端点尚不存在的边。
- `nullValues` 中列出的字符串会被当作空值处理，写入时会跳过这些属性。
- 时区通过**每个 mapping** 的 `timeZone` 配置（sink 没有顶层 `time_zone` 选项，与 HugeGraph Source 不同，因为日期解析属于每个 mapping 的行为）。省略时默认使用 Worker JVM 时区，与 Source 一致，从而保证 Source→Sink 往返时绝对时间不变。

## 使用示例

下面示例使用默认的 `schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST`。如果设置 `schema_save_mode = ERROR_WHEN_SCHEMA_NOT_EXIST`，请在运行任务前先创建好对应 HugeGraph Schema。

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
    mappings = [
      {
        type = "VERTEX"
        label = "person"
        idStrategy = "PRIMARY_KEY"
        idFields = ["name"]
        properties = ["name", "age"]
      }
    ]
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
    mappings = [
      {
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
        fieldMapping = {
          person1_name = "name"
          person2_name = "name"
        }
      }
    ]
  }
}
```

### 3. 写入 DELETE 行

Sink 会按行 kind 处理数据。`DELETE` 行只需要提供重建元素 id 所需的字段，其他列可以省略。把 `delete_vertex_with_edges` 设为 `true` 后，删除顶点时会同时删除其相连的边。

```hocon
source {
  FakeSource {
    schema = {
      fields = {
        name = "string"
      }
    }
    rows = [
      {
        kind = DELETE
        fields = ["bob"]
      }
    ]
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    delete_vertex_with_edges = true
    mappings = [
      {
        type = "VERTEX"
        label = "person"
        idStrategy = "PRIMARY_KEY"
        idFields = ["name"]
      }
    ]
  }
}
```

### 4. 从 HugeGraph Source 整体克隆

当上游是 HugeGraph Source 时，每行已经带有预留列（顶点为 `~id`，边端点为 `~source_id` / `~target_id`），直接复用这些 id 即可完整还原图。下面的示例把 `multi_table_sink_replica` 调大，让 sink 在 source 读取多 label 时能并行写出。

```hocon
env {
  job.mode = "BATCH"
}

source {
  HugeGraph {
    host = "src-host"
    port = 8080
    graph_name = "hugegraph"
    label_type = "VERTEX"
  }
}

sink {
  HugeGraph {
    host = "dst-host"
    port = 8080
    graph_name = "hugegraph"
    multi_table_sink_replica = 2
    batch_size = 500
    mappings = [
      {
        type = "VERTEX"
        label = "person"
        idStrategy = "CUSTOMIZE_STRING"
        idFields = ["~id"]
      }
    ]
  }
}
```

## Changelog

<ChangeLog />

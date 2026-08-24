import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable Sink 连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>

## 描述

使用原生 Bigtable Data v2 Java 客户端将数据写入 Google Cloud Bigtable。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 参数

| 参数名               | 类型    | 是否必填 | 默认值 |
|--------------------|---------|--------|------|
| project_id         | string  | 是     | -    |
| instance_id        | string  | 是     | -    |
| table              | string  | 是     | -    |
| rowkey_column      | list    | 是     | -    |
| column_family      | config  | 是     | -    |
| credentials_path   | string  | 否     | -    |
| rowkey_delimiter   | string  | 否     | ""   |
| version_column     | string  | 否     | -    |
| null_mode          | string  | 否     | skip |
| batch_mutation_size| int     | 否     | 100  |
| schema_save_mode   | enum    | 否     | RECREATE_SCHEMA |
| data_save_mode     | enum    | 否     | APPEND_DATA |
| multi_table_sink_replica | int | 否    | 1    |
| common-options     |         | 否     | -    |

### project_id [string]

Google Cloud 项目 ID，例如 `"my-gcp-project"`。

### instance_id [string]

Bigtable 实例 ID，例如 `"my-bigtable-instance"`。

### table [string]

写入的 Bigtable 表名，例如 `"my-table"`。连接器不会自动建表，需要先在 Bigtable 中创建好目标表以及会用到的列族。

### rowkey_column [list]

用于构造行键的列名列表，例如 `["id"]` 或 `["tenant_id", "event_id"]`。多列时用 `rowkey_delimiter` 拼接。当只有一个行键列时，值为 `null` 或空字符串会让作业直接以 `WRITE_FAILED` 失败；当配置了多个行键列时，非末尾列为 `null` 会被静默转成空串并通过 `rowkey_delimiter` 拼接到组合行键中，只有当整条组合行键最终为空时作业才会失败。

### column_family [config]

列名到列族的映射配置。可使用 `all_columns` 作为默认列族：

```hocon
column_family {
  all_columns = "cf"
}
```

也可以为不同列指定不同列族：

```hocon
column_family {
  name = "info"
  age  = "stats"
}
```

未在映射中出现的字段名会回退到 `all_columns` 指定的列族；如果也没有 `all_columns`，则使用默认列族 `cf`。

### credentials_path [string]

Google Cloud 服务账号 JSON 密钥文件路径。未设置时使用应用默认凭证（ADC）。

### rowkey_delimiter [string]

多列行键的拼接分隔符，默认为空字符串 `""`。

### version_column [string]

用作 Bigtable Cell 时间戳（微秒）的 BIGINT 列名。未设置时使用当前系统时间。

### null_mode [string]

空值写入策略：`skip`（默认，跳过该 Cell）或 `empty`（写入空字节数组）。

### batch_mutation_size [int]

每次批量提交的行数，默认 `100`。调大该值可以提高吞吐，但会增加每个任务本地缓存的数据量。

### schema_save_mode [enum]

Schema 保存模式。当前只支持 `RECREATE_SCHEMA`。

连接器不会自动创建 Bigtable 表或列族。运行作业前，需要先在 Bigtable 中创建好目标表和所有会用到的列族。

### data_save_mode [enum]

数据保存模式。当前只支持 `APPEND_DATA`。

本连接器暂不支持 `DROP_DATA` 和 `ERROR_WHEN_DATA_EXISTS`。如果需要干净的目标表，请在运行作业前手动清空或重建 Bigtable 表。

### multi_table_sink_replica [int]

多表写入时使用的 Sink 副本数。`multi_table_sink_replica` 用于在单个 Sink 实例中增加并行写入副本数；目标 Bigtable 表由 `table` 选项固定，不会根据上游表名动态切换。更多说明请参考 [Sink Common Options](../common-options/sink-common-options.md)。

### common options

Sink 插件通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。

## 数据类型映射

Bigtable 没有关系型数据库那样的列类型。连接器会按如下格式把 SeaTunnel 字段写入 Bigtable Cell：

| SeaTunnel 类型 | Bigtable 中的保存格式 |
|----------------|----------------------|
| TINYINT        | 1 字节二进制 |
| SMALLINT       | 2 字节大端二进制 |
| INT            | 4 字节大端二进制 |
| BIGINT         | 8 字节大端二进制 |
| FLOAT          | 4 字节 IEEE 754 大端二进制 |
| DOUBLE         | 8 字节 IEEE 754 大端二进制 |
| BOOLEAN        | 1 字节，`1` 表示 true，`0` 表示 false |
| BYTES          | 原始字节 |
| STRING         | UTF-8 文本 |
| DECIMAL        | UTF-8 普通数字字符串 |
| DATE           | UTF-8 `yyyy-MM-dd` |
| TIME           | UTF-8 `HH:mm:ss` |
| TIMESTAMP      | UTF-8 `yyyy-MM-dd HH:mm:ss` |

:::tip

Sink 会把非行键字段写成 Bigtable Cell。目标列族由 `column_family` 决定，Bigtable 的列限定符使用 SeaTunnel 字段名。每条上游记录都会被当成无条件的 Cell 变更，所以 `UPDATE` / `DELETE` 类型的行不会被解释为 CDC 操作，而是直接覆盖相同 `(行键, 列族, 列限定符)` 下的旧 Cell。

:::

## 任务示例

### 使用应用默认凭证写入

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id  = "my-gcp-project"
    instance_id = "my-bigtable-instance"
    table       = "events"
    rowkey_column = ["event_id"]
    column_family {
      all_columns = "cf"
    }
  }
}
```

### 使用服务账号和复合行键写入

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id       = "my-gcp-project"
    instance_id      = "my-bigtable-instance"
    table            = "events"
    credentials_path = "/secrets/sa-key.json"
    rowkey_column    = ["tenant_id", "event_id"]
    rowkey_delimiter = "#"
    column_family {
      all_columns = "data"
    }
    batch_mutation_size = 500
  }
}
```

### 写入多个列族

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id  = "my-gcp-project"
    instance_id = "my-bigtable-instance"
    table       = "user_profile"
    rowkey_column = ["user_id"]
    column_family {
      name       = "identity"
      email      = "identity"
      age        = "stats"
      last_login = "stats"
    }
  }
}
```

### 使用版本列并把空值写成空字节

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id       = "my-gcp-project"
    instance_id      = "my-bigtable-instance"
    table            = "events"
    rowkey_column    = ["tenant_id", "event_id"]
    rowkey_delimiter = "#"
    version_column   = "event_ts"
    null_mode        = "empty"
    column_family {
      all_columns = "data"
      event_type  = "meta"
    }
  }
}
```

### 流式写入并按 Checkpoint 刷新

在流式模式下，Writer 会在每次 checkpoint 触发时把本地 mutation 缓冲写入 Bigtable。`batch_mutation_size` 仍然控制任务内部缓冲，checkpoint 频率只会影响已缓冲的 mutation 多久被发送到 Bigtable。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 1000
    schema {
      fields {
        tenant_id  = string
        event_id   = string
        event_ts   = bigint
        event_type = string
        payload    = string
      }
    }
    plugin_output = "events_stream"
  }
}

sink {
  GoogleBigtable {
    plugin_input = "events_stream"
    project_id   = "my-gcp-project"
    instance_id  = "my-bigtable-instance"
    table        = "events"
    credentials_path = "/secrets/sa-key.json"
    rowkey_column    = ["tenant_id", "event_id"]
    rowkey_delimiter = "#"
    version_column   = "event_ts"
    column_family {
      all_columns = "data"
      event_type  = "meta"
    }
    batch_mutation_size = 200
  }
}
```

## Changelog

<ChangeLog />

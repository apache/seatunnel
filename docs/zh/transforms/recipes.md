# Transform Recipes（场景化配置示例）

Transform 文档通常是“单个 Transform 插件”的参数说明，但实际落地时，用户更需要能直接复制的“场景级 recipe”，把下面三件事串起来：

- Source 如何把原始数据解析成一行（Row）
- Transform 如何抽取/清洗/转换字段
- Sink 对 Schema/类型的要求（尤其是向量库）

本页提供一些常见场景的可复制示例，便于你快速改成自己的作业配置。

## Recipe 1：解析深层嵌套 JSON（Kafka/File）并写入 MySQL

### 适用场景

- 上游数据是一个嵌套较深的 JSON（Kafka message value 或 JSON lines 文件）。
- 你只需要把其中少量字段抽出来，映射为“扁平列”，写入关系型数据库表。

### 关键点：把原始 JSON 保留成单列

JsonPath transform 是从某一个字段（STRING/BYTES）里读取 JSON 并做 JsonPath 抽取的。

- Kafka：通常意味着你需要把 message value 作为一个字段保留下来（例如 STRING/BYTES）。
- File：可以用 `LocalFile` 的 `text` 格式，让每行内容落到默认的 `content` 列上。

#### Kafka 示例：把 message value 保留成单列

如果你希望对 Kafka 的 message value 使用 `JsonPath`，请确保 Kafka Source 能把“整段 message value”输出成单列。一个简单做法是使用 `format = text` 且不配置 `schema`，这样 Kafka 会输出一个名为 `content` 的单列字符串字段。

```hocon
source {
  Kafka {
    plugin_output = "kafka_raw"
    topic = "events"
    bootstrap.servers = "kafka:9092"
    format = text
  }
}

transform {
  JsonPath {
    plugin_input = "kafka_raw"
    plugin_output = "kafka_flat"
    columns = [
      {
        src_field = "content"
        path = "$.event_id"
        dest_field = "event_id"
        dest_type = "string"
      }
    ]
  }
}
```

如果你使用 `format = NATIVE`，message value 字段名为 `value`（bytes），此时 `src_field` 应配置为 `value`。

### 完整示例（LocalFile JSON lines -> JsonPath -> JDBC sink）

假设输入文件每行一个 JSON 对象，例如：

```json
{"event_id":"e1","payload":{"user":{"id":1001,"name":"alice"},"order":{"amount":12.34,"currency":"USD"}}}
```

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  LocalFile {
    plugin_output = "events_raw"
    path = "/data/events.jsonl"
    file_format_type = "text"
  }
}

transform {
  JsonPath {
    plugin_input = "events_raw"
    plugin_output = "events_flat"

    row_error_handle_way = "SKIP"
    columns = [
      {
        src_field = "content"
        path = "$.event_id"
        dest_field = "event_id"
        dest_type = "string"
      },
      {
        src_field = "content"
        path = "$.payload.user.id"
        dest_field = "user_id"
        dest_type = "bigint"
      },
      {
        src_field = "content"
        path = "$.payload.user.name"
        dest_field = "user_name"
        dest_type = "string"
      },
      {
        src_field = "content"
        path = "$.payload.order.amount"
        dest_field = "order_amount"
        dest_type = "double"
      },
      {
        src_field = "content"
        path = "$.payload.order.currency"
        dest_field = "currency"
        dest_type = "string"
      }
    ]
  }
}

sink {
  Jdbc {
    plugin_input = "events_flat"
    url = "jdbc:mysql://mysql:3306/demo"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "root"
    database = "demo"
    table = "events_flat"
  }
}
```

### 1 行输入能否变成多行输出？

Transform V2 的 SPI 设计上同时支持：

- map（1 行输入 -> 1 行输出）
- flatMap（1 行输入 -> N 行输出）

但目前文档层面并没有一个“内置的 explode（把数组字段拆成多行）” transform。如果你的 JSON 里有 `items: [...]` 这类数组字段，并希望把每个 item 展开成独立行，通常有以下选择：

- 在上游预处理（例如 Flink/Spark SQL，或由上游生产端直接输出“一行一条目标记录”的 JSON）。
- 写一个自定义 Transform V2（flatMap）插件来实现 1:N 拆分。
- 如果只是“拆成多列（仍然 1:1）”，可以优先用内置的 `JsonPath`、`Split`、`Sql`、`FieldMapper` 等。

## Recipe 2：把 MySQL 数据转换为 Milvus FLOAT_VECTOR

### Milvus 侧的类型要求

Milvus 的 `FLOAT_VECTOR` 对应 SeaTunnel 的 `FLOAT_VECTOR` 逻辑类型（不是 `array<float>`）。在 SeaTunnel 运行时，`FLOAT_VECTOR` 的值类型是 `ByteBuffer`。

### 方案 A（推荐）：Embedding transform 直接对文本向量化

如果你有原始文本（或其他 Embedding 支持的模态），希望在链路中直接生成向量，推荐使用 `Embedding` transform。它会新增一个 `FLOAT_VECTOR` 列，并根据模型响应自动设置向量维度。

`enable_auto_id = true` 只有在上游 schema 已经包含主键信息时才会生效。这个示例里，`id` 应该是源表的真实主键，且 JDBC source 需要把该主键元信息保留下来。如果你的 `query` 或后续 transform 丢失了主键信息，请提前创建好 Milvus collection，或在上游 schema 中显式保留主键。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Jdbc {
    plugin_output = "mysql_docs"
    url = "jdbc:mysql://mysql:3306/demo"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "root"
    query = "select id, content from demo.documents"
  }
}

transform {
  Embedding {
    plugin_input = "mysql_docs"
    plugin_output = "mysql_docs_with_vec"

    model_provider = "OPENAI"
    model = "text-embedding-3-small"
    api_key = "sk-xxx"

    vectorization_fields {
      content_vector = content
    }
  }
}

sink {
  Milvus {
    plugin_input = "mysql_docs_with_vec"
    url = "http://milvus:19530"
    token = "<your-token>"
    collection = "documents"
    enable_auto_id = true
  }
}
```

### 方案 B：MySQL JSON 数组 -> FLOAT_VECTOR（需要自定义转换）

如果你的 MySQL 表里已经存了 embedding（例如 JSON 字符串：`"[0.12, 0.98, ...]"`），仍需要把它转换成 SeaTunnel 的 `FLOAT_VECTOR`（`ByteBuffer`）。目前文档层面没有一个专门的“JSON 数组转 FLOAT_VECTOR”的内置 transform。

你可以选择：

- 写自定义 Transform V2 插件来做类型转换；
- 或使用 `DynamicCompile` 在作业里内联代码完成解析与 `ByteBuffer` 构造。

这里同样适用主键规则：`enable_auto_id = true` 不会自动创建主键。上游 schema 仍然需要提供真实主键列，例如 `id`。

下面给出一个使用 `DynamicCompile`（Groovy）的完整示例：从 MySQL 读取 JSON 字符串列 `embedding_json`，并转换成 `FLOAT_VECTOR` 列 `content_vector`（示例维度为 4，请按实际 embedding 维度调整）。

该示例通过列下标读取 `embedding_json`（`inputRow.getField(1)`），因此请保持 `query` 列顺序为 `id, embedding_json`，或自行调整下标。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Jdbc {
    plugin_output = "mysql_embeddings"
    url = "jdbc:mysql://mysql:3306/demo"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "root"
    query = "select id, embedding_json from demo.embeddings"
  }
}

transform {
  DynamicCompile {
    plugin_input = "mysql_embeddings"
    plugin_output = "mysql_embeddings_vec"
    compile_language = "GROOVY"
    compile_pattern = "SOURCE_CODE"
    source_code = """
      import org.apache.seatunnel.api.table.catalog.Column
      import org.apache.seatunnel.api.table.catalog.CatalogTable
      import org.apache.seatunnel.api.table.catalog.PhysicalColumn
      import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor
      import org.apache.seatunnel.api.table.type.VectorType
      import org.apache.seatunnel.common.utils.JsonUtils
      import org.apache.seatunnel.common.utils.VectorUtils
      import java.util.List

      class demo {
        Integer dim = 4

        public Column[] getInlineOutputColumns(CatalogTable inputCatalogTable) {
          PhysicalColumn vectorCol =
            PhysicalColumn.of("content_vector", VectorType.VECTOR_FLOAT_TYPE, null, dim, true, null, "")
          return new Column[] { vectorCol }
        }

        public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow) {
          Object json = inputRow.getField(1)
          if (json == null) {
            return new Object[] { null }
          }
          List<Float> list = JsonUtils.toList(json.toString(), Float.class)
          if (list.size() != dim) {
            throw new IllegalArgumentException("embedding dimension mismatch, expected " + dim + " but got " + list.size())
          }
          Float[] floats = list.toArray(new Float[0])
          return new Object[] { VectorUtils.toByteBuffer(floats) }
        }
      }
    """
  }
}

sink {
  Milvus {
    plugin_input = "mysql_embeddings_vec"
    url = "http://milvus:19530"
    token = "<your-token>"
    collection = "embeddings"
    enable_auto_id = true
  }
}
```

## LLM / 文件内容处理边界（内置支持 vs 需要前置处理）

- `LLM` transform 是把一行里的字段作为输入发送给模型推理，它不会自动读取文件路径、下载 URL，也不会自动对大文件做切片/分块。
- 如果要处理大文件或二进制内容，建议在进入 `LLM` transform 之前完成解析与切片（例如用文件类 connector 读成文本行，或在上游预先把内容切成多行）。

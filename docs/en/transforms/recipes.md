# Transform Recipes

The transform docs describe individual plugins, but real jobs often need scenario-level configurations that combine:

- Source parsing (how raw data becomes a row)
- Transforms (how fields are extracted/converted)
- Sink schema requirements (especially for vector databases)

This page provides practical “recipes” you can copy and adapt.

## Recipe 1: Extract deeply nested JSON (Kafka/File) and write to MySQL

### When to use

- Your upstream data is a nested JSON payload (Kafka message value or a JSON-lines file).
- You want to map a few nested fields into flat columns and write them to a relational table.

### Key point: keep the original JSON as a single column

The JsonPath transform reads JSON from a single source field (STRING/BYTES). For Kafka, that typically means you keep the message value as a single field; for files, you can read each line as a single `content` field.

#### Kafka example: keep message value as one column

If you want to apply `JsonPath` on Kafka message values, make sure the Kafka source outputs the whole message value as a single field. A simple way is to use `format = text` and do not configure `schema`, then Kafka will output a single string column named `content`.

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

If you use `format = NATIVE`, the message value field is `value` (bytes), and `src_field` should be `value`.

### Complete example (LocalFile JSON lines -> JsonPath -> JDBC sink)

Assume the input file contains one JSON object per line, for example:

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

### One input row -> multiple output rows?

SeaTunnel’s Transform V2 SPI supports both:

- map (1 input row -> 1 output row)
- flatMap (1 input row -> N output rows)

However, there is no documented built-in “explode array into many rows” transform today. If your JSON contains an array field like `items: [...]` and you want to turn each item into a separate output row, you typically have these options:

- Pre-process upstream (e.g., Flink/Spark SQL, or upstream producers) to output one JSON object per target row.
- Write a custom Transform V2 plugin that implements a flatMap transform.
- If you only need to split into multiple columns (still 1 row -> 1 row), use built-in transforms such as `JsonPath`, `Split`, `Sql`, `FieldMapper`.

## Recipe 2: Convert MySQL data to Milvus FLOAT_VECTOR

### What Milvus expects

Milvus’s `FLOAT_VECTOR` maps to SeaTunnel’s `FLOAT_VECTOR` logical type (not `array<float>`). In SeaTunnel’s runtime, `FLOAT_VECTOR` is represented as a `ByteBuffer`.

### Option A (recommended): vectorize text with the Embedding transform

If you have raw text (or other supported modality) and you want to generate vectors in the pipeline, use the `Embedding` transform. It will add a new `FLOAT_VECTOR` column and set the vector dimension automatically based on the model response.

`enable_auto_id = true` only works when the upstream schema already contains a primary key. In this example, `id` should be the real primary key of the source table, and the JDBC source should preserve that primary key metadata. If your `query` or later transforms lose primary key metadata, create the Milvus collection in advance or keep the primary key in the upstream schema explicitly.

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

### Option B: MySQL JSON array -> FLOAT_VECTOR (requires custom conversion)

If your MySQL table already stores embeddings as a JSON array (e.g. `"[0.12, 0.98, ...]"`), you still need to convert it to SeaTunnel’s `FLOAT_VECTOR` representation (`ByteBuffer`). There is no dedicated built-in “JSON array to FLOAT_VECTOR” transform documented today.

You can implement the conversion with a custom Transform V2, or use `DynamicCompile` to parse the JSON string and build a `ByteBuffer` vector.

The same primary key rule applies here: `enable_auto_id = true` does not create a primary key automatically. The upstream schema still needs a real primary key column such as `id`.

Below is a complete example using `DynamicCompile` (Groovy). It reads a JSON string column `embedding_json` from MySQL and converts it to a `FLOAT_VECTOR` column `content_vector` (dimension = 4 in this example).

This example reads `embedding_json` by column position (`inputRow.getField(1)`), so keep the `query` column order as `id, embedding_json`, or update the index accordingly.

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

## LLM/file-content boundaries (what’s built-in vs what you must do before)

- The `LLM` transform sends row fields to the model as inference input; it does not automatically read file paths, download URLs, or chunk large binaries.
- For large files or binary content, do parsing/chunking before the `LLM` transform (for example, use file connectors to read text, or pre-split content into manageable rows).

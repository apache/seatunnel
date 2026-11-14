# TextChunk

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark

## Description

TextChunk splits a long text field into multiple smaller chunks and emits **one row per chunk** (a 1-row → N-rows transform). Each output row keeps **all of the original source fields** unchanged and carries two additional columns: the chunk text and its 0-based sequence index within the source document. Use it to prepare long documents for consumers that work on bounded text units, for example embedding pipelines that ingest fixed-size passages.

The chunk column is a STRING column and the index column is an INT column. Each of them is appended at the end of the schema, or reused when a column with the same name already exists. All chunks of one input row share the same source key, so the transform extends the primary key and every unique key with the chunk index column to keep every chunk unique for key-aware sinks.

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| text_field | String | Conditional | - | The source text field to split into chunks. Required by the transform-level rule and by every `table_transform` entry. |
| output_field | String | No | `chunk` | Output column holding each chunk (STRING). Must not match a primary/unique key column and must differ from `chunk_index_field`. |
| chunk_index_field | String | No | `chunk_index` | Output column holding the 0-based chunk index within the document (INT). Same restrictions as `output_field`. |
| chunk_size | Int | No | `1000` | Maximum chunk length in Unicode code points. Must be greater than `0`. |
| overlap_size | Int | No | `0` | Overlap carried into the start of the next chunk, in Unicode code points. Must satisfy `0 <= overlap_size < chunk_size`. |
| separators | Array&lt;String&gt; | No | `["\n\n", "\n", "。", "！", "？", ". ", " "]` | Separators tried in priority order to avoid cutting mid-sentence. An empty list falls back to fixed-size splitting. |
| skip_empty_text | Boolean | No | `true` | Drop rows whose text is null or empty instead of passing them through. |
| table_match_regex | String | No | `.*` | Match upstream table paths to which the transform-level rule applies. |
| table_transform | Array&lt;Map&gt; | No | `[]` | Complete rules for individual tables, selected by an exact `table_path`. |
| rule_match_mode | Enum | No | `FIRST_MATCH` | Select the first matching per-table rule, or apply all matching rules in order. See the duplicate-path behavior below. |
| common options | - | No | - | Common transform options such as `plugin_input` and `plugin_output`. |

### text_field [String]

The source text field to split into chunks. The field must exist in the input schema; otherwise the transform fails with a field-not-found error.

Without a nonempty `table_transform` list, `text_field` is required at the transform level. An empty `table_transform = []` does not remove this requirement.

With per-table entries, the transform-level rule may be absent. The presence of `text_field` marks the transform-level rule as configured: when it is not set at the transform level, the other transform-level options have no effect and only the `table_transform` entries apply. Each entry must set its own `text_field` and is validated on its own at submission time with default values applied.

### output_field [String]

The name of the output column holding each chunk (type STRING). If a column with this name already exists it is reused (its type is replaced with STRING), otherwise a nullable column is appended at the end of the schema. Defaults to `chunk`.

The name must not match a column that takes part in a primary key or a unique key: the transform rejects such a configuration with an error, because writing per-chunk values into a key column would let chunks from different source rows collide in key-aware sinks. Reusing a non-key column (for example writing the chunk back into the source text column) is allowed. The name must also differ from `chunk_index_field`.

### chunk_index_field [String]

The name of the output column holding the chunk sequence index within a document (type INT, 0-based, not null). If a column with this name already exists it is reused, otherwise it is appended at the end of the schema. Defaults to `chunk_index`.

The same key-column restriction as `output_field` applies, and the name must differ from `output_field`.

The rows produced from one input row all carry the same source key, so the transform appends this column to the primary key and to every unique key of the produced table (unless the key is auto-generated or already contains the column). This keeps each chunk unique for key-aware sinks.

### chunk_size [Int]

Maximum length of each chunk, counted in **Unicode code points** — a supplementary character such as an emoji counts as one. (A combining sequence or a ZWJ-joined emoji still counts as several code points and may be split between them.) Must be greater than `0`. Defaults to `1000`.

Each produced chunk is at most `chunk_size` code points long and the carried overlap counts toward that budget, so a surrogate pair (e.g. an emoji) is never split in half.

### overlap_size [Int]

Overlap length between adjacent chunks, counted in **Unicode code points**. Carrying a little context from the end of one chunk into the start of the next helps preserve meaning across chunk boundaries. The overlap is made up of **whole trailing pieces** (complete words/sentences), so it never begins in the middle of a word; `overlap_size` is therefore an upper bound — the actual overlap rounds down to whole pieces, and is empty when even the last single piece is longer than the budget. (In the empty-`separators` fixed-size fallback there are no pieces, so the overlap is a plain `overlap_size`-code-point window and is not rounded to a word boundary.) Must satisfy `0 <= overlap_size < chunk_size`. Defaults to `0`.

:::caution
The number of chunks grows roughly as `chunk_size / (chunk_size - overlap_size)`, so an `overlap_size` close to `chunk_size` multiplies the output row count (and memory) dramatically — for example `chunk_size = 1000` with `overlap_size = 999` produces about one chunk per code point. The transform logs a runtime warning when `overlap_size` reaches half of `chunk_size`. Keep the overlap well below `chunk_size`.
:::

### separators [Array&lt;String&gt;]

Separators tried in priority order to avoid cutting mid-sentence: the text is split by the first separator, and any piece still longer than `chunk_size` is further split by the next separator, and so on. If every separator has been tried and a piece is still longer than `chunk_size`, that piece is split into fixed-size windows, which then take part in the normal merge and overlap step like any other piece. If the list is left empty, the whole text falls back to fixed-size splitting. Defaults to `["\n\n", "\n", "。", "！", "？", ". ", " "]`.

Separators are matched literally (not as regular expressions) and are retained: each separator stays attached to the end of the piece it follows, so merging pieces back into a chunk keeps the spaces and newlines between them (words split on `" "` are re-joined as `a b`, not `ab`), and runs of consecutive separators (e.g. blank lines) are preserved. With `overlap_size = 0` the chunks concatenate back to the original text.

### skip_empty_text [Boolean]

Controls how a row whose `text_field` is `null` or empty is handled. When `true` (the default) the row produces no output rows; when `false` the row is passed through with `output_field` set to `null` and `chunk_index_field` set to `0`.

### table_match_regex [String]

A Java regular expression matched against the whole upstream table path, such as `test.abc` or `database.schema.table`. The default `.*` matches all table paths. This is the actual table path, not the dataset name in `plugin_input` or `plugin_output`.

This option selects the tables to which the transform-level rule applies. It does not restrict explicit `table_transform` entries, and without a transform-level rule (see `text_field`) it does not create a chunking rule on its own.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules; the default is an empty list. Each entry must have a nonblank `table_path` (an exact path, not a regular expression) in the form `databaseName[.schemaName].tableName`, plus a complete single-table TextChunk rule for that table: `text_field` and any optional chunking options.

An entry is a complete rule and does not inherit the transform-level options. A matching entry takes precedence even when its table does not match `table_match_regex`. Every entry is validated on its own at submission time with default values applied, so it must satisfy `chunk_size > 0` and `0 <= overlap_size < chunk_size` even when it relies on the defaults.

### rule_match_mode [Enum]

Controls multiple entries with the same exact `table_path`:

- Explicit `FIRST_MATCH`: use the first matching entry in list order.
- Explicit `ALL_MATCH`: apply all matching entries in list order. TextChunk is a 1-row-to-N-rows transform, so the rows produced by an earlier matching entry become the input of the next matching entry for the same table.

Although the option's default is `FIRST_MATCH`, omitting the option rejects duplicate `table_path` entries during transform initialization. Set it explicitly if repeated paths are intentional. With unique paths, the default is sufficient.

### common options

Use `plugin_input` and `plugin_output` to connect this transform to upstream and downstream plugins. See [Transform Plugin](common-options/common-options.md) for common parameters.

## Multi-Table Configuration

For each input table, the effective rule is selected in this order:

1. Use the matching `table_transform` entry or entries, according to `rule_match_mode`.
2. Otherwise, apply the transform-level rule if one is configured (see `text_field`) and the table path matches `table_match_regex`.
3. Otherwise, pass the table's rows through unchanged.

In a `table_transform`-only configuration, unlisted tables pass through unchanged. A table that passes through unchanged also keeps its original schema: the chunk and chunk index columns are added only by the tables that actually get a chunking rule.

The following examples cover all three configuration shapes. See [Multi-Table Transform](transform-multi-table.md) for the shared matching rules.

## Examples

These complete batch configurations use FakeSource and Console. Install those connectors in your SeaTunnel distribution before running a saved configuration with `bin/seatunnel.sh --config <file> -e local`. The rows are fixed by the `rows` option of FakeSource, so the expected outputs are deterministic.

### Transform-level rule only

Input: one row in table `test.doc`, with a 59-code-point text. With `separators = [" "]`, `chunk_size = 20` and `overlap_size = 6`, the text is split into space-separated pieces that are greedily merged into chunks of at most 20 code points, carrying at most 6 code points of whole trailing pieces into the next chunk:

| id | content | chunk | chunk_index |
|----|---------|-------|-------------|
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `SeaTunnel is fast. ` | 0 |
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `fast. SeaTunnel is ` | 1 |
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `is reliable. ` | 2 |
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `SeaTunnel scales.` | 3 |

The chunks retain their trailing separators, so `chunk 0` ends with the piece `fast. ` that is carried into `chunk 1`, and `chunk 1` ends with `is ` that is carried into `chunk 2` (`is reliable. `). The last chunk is `SeaTunnel scales.`.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    schema {
      fields {
        id = "int"
        content = "string"
      }
    }
    rows = [
      { fields = [1, "SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales."], kind = INSERT }
    ]
  }
}

transform {
  TextChunk {
    plugin_input = "fake"
    plugin_output = "chunked"
    text_field = "content"
    chunk_size = 20
    overlap_size = 6
    separators = [" "]
  }
}

sink {
  Console {
    plugin_input = "chunked"
  }
}
```

### Transform-level rule with per-table rules

Each input table contains one row. The explicit entry for `test.doc_a` overrides the transform-level rule. `test.doc_c` does not match `table_match_regex` and passes through unchanged.

| Table | Input rows | Output rows | Effective rule |
|-------|------------|-------------|----------------|
| `test.doc_a` | 1 | 2 | Per-table entry: `chunk_size = 2`, `separators = []`. |
| `test.doc_b` | 1 | 3 | Transform-level `chunk_size = 3` with the default separators. |
| `test.doc_c` | 1 | 1 | No matching rule; unchanged, without chunk columns. |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema {
          table = "test.doc_a"
          columns = [
            { name = "id", type = "int" },
            { name = "content", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "xxxx"], kind = INSERT }
        ]
      },
      {
        schema {
          table = "test.doc_b"
          columns = [
            { name = "id", type = "int" },
            { name = "content", type = "string" }
          ]
        }
        rows = [
          { fields = [2, "yy yy yy "], kind = INSERT }
        ]
      },
      {
        schema {
          table = "test.doc_c"
          columns = [
            { name = "id", type = "int" },
            { name = "content", type = "string" }
          ]
        }
        rows = [
          { fields = [3, "kept as is"], kind = INSERT }
        ]
      }
    ]
  }
}

transform {
  TextChunk {
    plugin_input = "fake"
    plugin_output = "chunked"
    text_field = "content"
    chunk_size = 3
    overlap_size = 0
    table_match_regex = "test[.](doc_a|doc_b)"
    table_transform = [
      {
        table_path = "test.doc_a"
        text_field = "content"
        chunk_size = 2
        overlap_size = 0
        separators = []
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "chunked"
  }
}
```

`test.doc_a` is chunked by its own entry into `xx` / `xx` (2 rows). `test.doc_b` falls back to the transform-level rule: the default separators split `yy yy yy ` into three pieces of 3 code points each, producing `yy ` / `yy ` / `yy ` (3 rows). `test.doc_c` has neither an entry nor a matching transform-level rule, so its single row passes through unchanged.

### Per-table rules only

There is no transform-level rule: `text_field` is not set at the transform level and the other transform-level options have no effect. Only the two listed tables are chunked; any other table would pass through unchanged.

| Table | Input rows | Output rows | Effective rule |
|-------|------------|-------------|----------------|
| `test.doc_a` | 1 | 2 | Entry: `chunk_size = 2`, `separators = []`. |
| `test.doc_b` | 1 | 3 | Entry: `chunk_size = 3`, `separators = []`. |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema {
          table = "test.doc_a"
          columns = [
            { name = "id", type = "int" },
            { name = "content", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "xxxx"], kind = INSERT }
        ]
      },
      {
        schema {
          table = "test.doc_b"
          columns = [
            { name = "id", type = "int" },
            { name = "content", type = "string" }
          ]
        }
        rows = [
          { fields = [2, "yy yy yy "], kind = INSERT }
        ]
      }
    ]
  }
}

transform {
  TextChunk {
    plugin_input = "fake"
    plugin_output = "chunked"
    table_transform = [
      {
        table_path = "test.doc_a"
        text_field = "content"
        chunk_size = 2
        overlap_size = 0
        separators = []
      },
      {
        table_path = "test.doc_b"
        text_field = "content"
        chunk_size = 3
        overlap_size = 0
        separators = []
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "chunked"
  }
}
```

`test.doc_a` produces `xx` / `xx` (2 rows) and `test.doc_b` produces the fixed-size windows `yy ` / `yy ` / `yy ` (3 rows).

> AI生成

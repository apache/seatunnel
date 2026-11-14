# 文本分块

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark

## 描述

TextChunk（文本分块）将一个长文本字段切分成多个较小的块（chunk），**每个块输出为一行**（1 行 → N 行的转换）。每个输出行都会**原样保留全部源字段**，并携带两个附加列：块文本，以及该块在源文档内的 0 起始序号。适合为按有限长度文本单元处理文档的下游场景做准备，例如按固定大小段落构建 embedding 的流水线。

块列为 STRING 类型，序号列为 INT 类型；两者都会追加到 schema 末尾，若已存在同名列则复用该列。同一输入行产生的所有块共享相同的源键，因此转换会把块序号列追加到主键和每一个唯一键上，保证对依赖键的 sink 而言每个块仍然唯一。

## 属性

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| text_field | String | 条件必需 | - | 需要切分成块的源文本字段。转换级规则和每个 `table_transform` 条目都必须设置。 |
| output_field | String | 否 | `chunk` | 存放每个块的输出列（STRING）。不能与主键/唯一键包含的列同名，且必须与 `chunk_index_field` 不同。 |
| chunk_index_field | String | 否 | `chunk_index` | 存放块在文档内 0 起始序号的输出列（INT）。限制与 `output_field` 相同。 |
| chunk_size | Int | 否 | `1000` | 每个块的最大长度，以 Unicode 码点计。必须大于 `0`。 |
| overlap_size | Int | 否 | `0` | 相邻块之间携带到下一块开头的重叠长度，以 Unicode 码点计。必须满足 `0 <= overlap_size < chunk_size`。 |
| separators | Array&lt;String&gt; | 否 | `["\n\n", "\n", "。", "！", "？", ". ", " "]` | 按优先级顺序尝试的分隔符，用于避免从句子中间切断。留空时回退为固定长度切分。 |
| skip_empty_text | Boolean | 否 | `true` | 文本为 null 或空字符串的行直接丢弃，而不是透传。 |
| table_match_regex | String | 否 | `.*` | 匹配要应用转换级规则的上游表路径。 |
| table_transform | Array&lt;Map&gt; | 否 | `[]` | 通过精确的 `table_path` 为各表指定完整规则。 |
| rule_match_mode | Enum | 否 | `FIRST_MATCH` | 选择第一条匹配的表级规则，或按顺序执行所有匹配的规则。重复路径的行为见下文。 |
| common options | - | 否 | - | `plugin_input`、`plugin_output` 等转换插件通用参数。 |

### text_field [String]

需要切分成块的源文本字段。该字段必须存在于输入 schema 中，否则转换会以"找不到字段"错误失败。

没有非空 `table_transform` 列表时，转换级必须设置 `text_field`。`table_transform = []` 不能免除这个要求。

配置了表级条目时，可以不设置转换级规则。转换级是否配置了规则以 `text_field` 是否存在为准：转换级未设置 `text_field` 时，其他转换级选项不产生任何作用，只有 `table_transform` 条目生效。每个条目必须设置自己的 `text_field`，并且在作业提交时会按应用默认值后的配置独立校验。

### output_field [String]

存放每个块的输出列名（类型 STRING）。若已存在同名列则复用该列（类型替换为 STRING），否则在 schema 末尾追加一个可空列。默认值为 `chunk`。

该列名不能与主键或唯一键所包含的列同名：转换会直接报错拒绝此类配置，因为用每块的值覆盖键列会导致不同源行的块在依赖键的 sink 中碰撞。复用非键列（例如把块写回源文本列）仍然允许。该列名还必须与 `chunk_index_field` 不同。

### chunk_index_field [String]

存放块在文档内序号的输出列名（类型 INT，从 0 开始，非空）。若已存在同名列则复用该列，否则在 schema 末尾追加。默认值为 `chunk_index`。

该列名受与 `output_field` 相同的键列限制，且必须与 `output_field` 不同。

同一输入行产生的多行都带有相同的源键，因此转换会把该列追加到输出表的主键和每一个唯一键上（键为自增主键或已包含该列时除外）。这保证对依赖键的 sink 而言每个块仍然唯一。

### chunk_size [Int]

每个块的最大长度，以 **Unicode 码点（code point）** 计——一个增补字符（如 emoji）计为一个码点。（组合字符或 ZWJ 连字 emoji 仍算作多个码点，可能在码点之间被切分。）必须大于 `0`。默认值为 `1000`。

每个产生的块长度不超过 `chunk_size` 个码点，携带的 overlap 也计入该上限，因此代理对（例如 emoji）不会被劈开。

### overlap_size [Int]

相邻块之间的重叠长度，以 **Unicode 码点（code point）** 计。把前一个块末尾的少量上下文带入下一个块开头，有助于在块边界处保留语义。重叠由**若干个完整的片段**（完整的词/句）组成，因此绝不会从单词中间开始；`overlap_size` 是一个上界——实际重叠会向下取整到完整片段，当连最后一个片段都超过该预算时则不携带任何重叠。（当 `separators` 留空、回退为固定长度切分时不存在"片段"，此时重叠为 `overlap_size` 个码点的定长窗口，不会向词边界取整。）必须满足 `0 <= overlap_size < chunk_size`。默认值为 `0`。

:::caution
块的数量大致按 `chunk_size / (chunk_size - overlap_size)` 增长，因此当 `overlap_size` 接近 `chunk_size` 时，输出行数（以及内存占用）会急剧膨胀——例如 `chunk_size = 1000` 且 `overlap_size = 999` 时，几乎每个码点都会产生一个块。当 `overlap_size` 达到 `chunk_size` 的一半时，转换会在运行时输出告警日志。请让 overlap 明显小于 `chunk_size`。
:::

### separators [Array&lt;String&gt;]

按优先级顺序尝试的分隔符，用于避免从句子中间切断：先用第一个分隔符切分，任何仍长于 `chunk_size` 的片段再用下一个分隔符继续切分，依此类推。当所有分隔符都已尝试而某个片段仍长于 `chunk_size` 时，该片段会被切成定长窗口，这些窗口随后与普通片段一样参与正常的合并与重叠步骤。若该列表留空，则整段文本回退为固定长度切分。默认值为 `["\n\n", "\n", "。", "！", "？", ". ", " "]`。

分隔符按字面量匹配（不是正则表达式）且会被保留：每个分隔符附着在它前面那一片的末尾，因此合并多片以填满一个块时，片与片之间的空格/换行会被保留（按 `" "` 切分的单词会重新拼成 `a b` 而不是 `ab`），连续的分隔符（例如空行）也会被保留。当 `overlap_size = 0` 时，各块拼接后可还原为原始文本。

### skip_empty_text [Boolean]

控制 `text_field` 为 `null` 或空字符串时该行的处理方式。为 `true`（默认）时该行不产生任何输出行；为 `false` 时该行被透传（`output_field` 置为 `null`、`chunk_index_field` 置为 `0`）。

### table_match_regex [String]

针对完整上游表路径匹配的 Java 正则表达式，例如 `test.abc` 或 `database.schema.table`。默认值 `.*` 匹配所有表路径。这里匹配的是实际表路径，不是 `plugin_input` 或 `plugin_output` 中的数据集名称。

此选项为转换级规则选择表，不限制显式的 `table_transform` 条目；没有转换级规则（见 `text_field`）时，仅设置此正则表达式也不会产生任何分块规则。

### table_transform [Array&lt;Map&gt;]

表级规则列表，默认为空列表。每个条目必须包含非空白的 `table_path`（精确路径，不是正则表达式，格式为 `databaseName[.schemaName].tableName`），以及该表的一条完整的单表 TextChunk 规则：`text_field` 及可选的分块选项。

每个条目都是完整规则，不继承转换级选项。匹配的条目具有更高优先级，即使该表不匹配 `table_match_regex` 也会使用该条目。每个条目在作业提交时会按应用默认值后的配置独立校验，因此即使依赖默认值，也必须满足 `chunk_size > 0` 且 `0 <= overlap_size < chunk_size`。

### rule_match_mode [Enum]

控制多个条目具有相同精确 `table_path` 时的行为：

- 显式设置 `FIRST_MATCH`：按列表顺序使用第一条匹配的条目。
- 显式设置 `ALL_MATCH`：按列表顺序执行所有匹配的条目。TextChunk 是 1 行 → N 行的转换，因此前一条匹配条目产生的行会作为同表下一条匹配条目的输入。

虽然该选项的默认值为 `FIRST_MATCH`，但省略此选项时，重复的 `table_path` 会在转换初始化阶段被拒绝。如果确实需要重复路径，请显式设置此选项。路径不重复时，使用默认值即可。

### common options

使用 `plugin_input` 和 `plugin_output` 连接上游和下游插件。通用参数详见[转换插件](common-options/common-options.md)。

## 多表配置

对于每个输入表，按照以下顺序选择有效规则：

1. 根据 `rule_match_mode` 使用匹配的一个或多个 `table_transform` 条目。
2. 否则，如果配置了转换级规则（见 `text_field`）且表路径匹配 `table_match_regex`，使用转换级规则。
3. 否则，原样传递该表的行。

仅配置 `table_transform` 时，未列出的表保持不变。原样传递的表也保持其原始 schema：只有实际获得分块规则的表才会追加块列和块序号列。

下面的示例涵盖三种配置方式。共享匹配规则详见[多表转换](transform-multi-table.md)。

## 示例

以下完整批处理配置使用 FakeSource 和 Console。在 SeaTunnel 发行包中安装这两个连接器后，可以将配置保存为文件，并通过 `bin/seatunnel.sh --config <file> -e local` 运行。行数据由 FakeSource 的 `rows` 选项固定，因此期望输出是确定的。

### 仅使用转换级规则

输入：表 `test.doc` 中的一行，文本共 59 个码点。在 `separators = [" "]`、`chunk_size = 20`、`overlap_size = 6` 下，文本先按空格切分，再贪心合并为不超过 20 个码点的块，并向下一块开头携带不超过 6 个码点的完整尾部片段：

| id | content | chunk | chunk_index |
|----|---------|-------|-------------|
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `SeaTunnel is fast. ` | 0 |
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `fast. SeaTunnel is ` | 1 |
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `is reliable. ` | 2 |
| 1 | SeaTunnel is fast. SeaTunnel is reliable. SeaTunnel scales. | `SeaTunnel scales.` | 3 |

各块保留了末尾分隔符：`chunk 0` 末尾的片段 `fast. ` 被携带到 `chunk 1` 开头，`chunk 1` 末尾的 `is ` 被携带到 `chunk 2` 开头（`is reliable. `）。最后一个块是 `SeaTunnel scales.`。

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

### 转换级规则 + 表级规则

每个输入表各一行。`test.doc_a` 的显式条目覆盖转换级规则。`test.doc_c` 不匹配 `table_match_regex`，原样透传。

| 表 | 输入行数 | 输出行数 | 有效规则 |
|----|----------|----------|----------|
| `test.doc_a` | 1 | 2 | 表级条目：`chunk_size = 2`、`separators = []`。 |
| `test.doc_b` | 1 | 3 | 转换级 `chunk_size = 3`，使用默认分隔符。 |
| `test.doc_c` | 1 | 1 | 无匹配规则；原样透传，不追加块列。 |

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

`test.doc_a` 按自己的条目切分为 `xx` / `xx`（2 行）。`test.doc_b` 使用转换级规则：默认分隔符把 `yy yy yy ` 切成 3 个各 3 码点的片段，产生 `yy ` / `yy ` / `yy `（3 行）。`test.doc_c` 既没有条目也不匹配转换级规则，因此它的一行原样透传。

### 仅使用表级规则

没有转换级规则：转换级未设置 `text_field`，其他转换级选项不产生任何作用。只有列出的两张表被分块；任何其他表都会原样透传。

| 表 | 输入行数 | 输出行数 | 有效规则 |
|----|----------|----------|----------|
| `test.doc_a` | 1 | 2 | 条目：`chunk_size = 2`、`separators = []`。 |
| `test.doc_b` | 1 | 3 | 条目：`chunk_size = 3`、`separators = []`。 |

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

`test.doc_a` 产生 `xx` / `xx`（2 行），`test.doc_b` 产生定长窗口 `yy ` / `yy ` / `yy `（3 行）。

> AI生成

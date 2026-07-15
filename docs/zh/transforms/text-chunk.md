# TextChunk

> 文本分块转换插件

## 描述

将一个长文本字段切分成多个较小的块（chunk），**每个块输出为一行**（1 行 → N 行的转换）。 每个输出行都会**原样保留全部源字段**，并追加两列：块文本，以及该块在源文档内的序号（从 0 开始）。

## 属性

|       名称         |   类型   | 是否必须 | 默认值 |
|-------------------|--------|------|-----|
| text_field        | string | yes  |     |
| output_field      | string | no   | chunk |
| chunk_index_field | string | no   | chunk_index |
| chunk_size        | int    | no   | 1000 |
| overlap_size      | int    | no   | 0   |
| separators        | array  | no   | ["\n\n", "\n", "。", "！", "？", ". ", " "] |
| skip_empty_text   | boolean| no   | true |

### text_field [string]

需要切分成块的源文本字段。

### output_field [string]

存放每个块的输出列名（类型 `STRING`）。若已存在同名列则复用该列，否则追加新列。默认值为 `chunk`。

### chunk_index_field [string]

存放块在文档内序号的输出列名（类型 `INT`，从 0 开始）。默认值为 `chunk_index`。

### chunk_size [int]

每个块的最大长度，以 **UTF-16 code unit**（Java `char`）计，而非字符数——一个字符（如 emoji）
可能占用多个 UTF-16 code unit。必须大于 `0`。默认值为 `1000`。

### overlap_size [int]

相邻块之间的重叠长度，以 **UTF-16 code unit** 计。把前一个块末尾的少量上下文带入下一个块开头，有助于在块边界处保留语义。重叠由
**若干个完整的片段**（完整的词/句）组成，因此绝不会从单词中间开始；`overlap_size` 是一个上界——实际重叠会向下取整到完整片段，
当连最后一个片段都超过该预算时，则不携带任何重叠。（当 `separators` 留空、回退为固定长度切分时不存在"片段"，此时重叠为
`overlap_size` 个字符的定长窗口，不会向词边界取整。）必须满足 `0 <= overlap_size < chunk_size`。默认值为 `0`。

> 注意：块的数量大致按 `chunk_size / (chunk_size - overlap_size)` 增长，因此当 `overlap_size` 接近
> `chunk_size` 时，输出行数（以及内存占用）会急剧膨胀——例如 `chunk_size = 1000` 且 `overlap_size = 999`
> 时，几乎每个字符都会产生一个块。请让 overlap 明显小于 `chunk_size`。

### separators [array]

按优先级顺序尝试的分隔符，用于避免从句子中间切断：先用第一个分隔符切分，任何仍长于 `chunk_size` 的片段再用
下一个分隔符继续切分，依此类推。若该列表留空，则回退为固定长度切分。默认值为
`["\n\n", "\n", "。", "！", "？", ". ", " "]`。

### skip_empty_text [boolean]

控制 `text_field` 为 `null` 或空字符串时的行处理逻辑。为 `true` 时该行被丢弃，为 `false` 时该行透传
（`output_field` 置为 `null`、`chunk_index_field` 置为 `0`），默认值为 `true`。

### common options [string]

转换插件的常见参数, 请参考 [Transform Plugin](common-options/common-options.md) 了解详情。

## 行为说明

- 当 `skip_empty_text = true`（默认）时，文本值为 `null` 或空字符串的输入行**不产生**任何输出行；设为 `skip_empty_text = false` 则该行会被透传（`chunk = null`、`chunk_index = 0`）。
- 每个产生的块长度不超过 `chunk_size` 个 UTF-16 code unit（携带的 overlap 也计入该上限）；为避免劈开代理对，某个块可能短最多 1 个 code unit。
- 分隔符会被保留：每个分隔符附着在它前面那一片的末尾，因此合并多片以填满一个块时，片与片之间的空格/换行会被保留（按 `" "` 切分的单词会重新拼成 `a b` 而不是 `ab`），连续的分隔符（例如空行）也会被保留。当 `overlap_size = 0` 时，各块拼接后可还原为原始文本。
- 同一输入行产生的多行都带有相同的源键（例如相同的 `id`）。为让它仍然唯一，转换会把 `chunk_index_field` 追加到主键和每个唯一键上。

## 示例

源端数据读取的表格如下：

| id |                    content                    |
|----|-----------------------------------------------|
| 1  | SeaTunnel is a data integration platform. ... |

我们想要将 `content` 字段切分成带重叠的块，可以像这样添加 `TextChunk` 转换：

```
transform {
  TextChunk {
    plugin_input = "fake"
    plugin_output = "fake1"
    text_field = "content"
    output_field = "chunk"
    chunk_index_field = "chunk_index"
    chunk_size = 200
    overlap_size = 20
  }
}
```

那么每个输入行会被展开为每块一行，写入结果表 `fake1`，并保留原始字段、追加 `chunk` / `chunk_index`：

| id |                    content                    |         chunk          | chunk_index |
|----|-----------------------------------------------|------------------------|-------------|
| 1  | SeaTunnel is a data integration platform. ... | SeaTunnel is a data... | 0           |
| 1  | SeaTunnel is a data integration platform. ... | ...platform that ...   | 1           |

## 作业配置示例

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 5
    schema = {
      fields {
        id = "int"
        content = "string"
      }
    }
  }
}

transform {
  TextChunk {
    plugin_input = "fake"
    plugin_output = "fake1"
    text_field = "content"
    output_field = "chunk"
    chunk_index_field = "chunk_index"
    chunk_size = 200
    overlap_size = 20
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## 更新日志

### 新版本

- 添加文本分块（TextChunk）转换插件

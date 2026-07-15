# TextChunk

> TextChunk transform plugin

## Description

Splits a long text field into multiple smaller chunks, emitting **one row per chunk** (a 1-row → N-rows
transform). Each output row keeps **all of the original source fields** unchanged, and appends two columns: the
chunk text and its 0-based sequence index within the source document.

## Options

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| text_field        | string | yes      |               |
| output_field      | string | no       | chunk         |
| chunk_index_field | string | no       | chunk_index   |
| chunk_size        | int    | no       | 1000          |
| overlap_size      | int    | no       | 0             |
| separators        | array  | no       | ["\n\n", "\n", "。", "！", "？", ". ", " "] |
| skip_empty_text   | boolean| no       | true          |

### text_field [string]

The source text field to split into chunks.

### output_field [string]

The name of the output column holding each chunk (type `STRING`). If a column with this name already
exists it is reused, otherwise a new column is appended. Defaults to `chunk`.

### chunk_index_field [string]

The name of the output column holding the chunk sequence index within a document (type `INT`,
0-based). Defaults to `chunk_index`.

### chunk_size [int]

Maximum length of each chunk, counted in **UTF-16 code units** (Java `char`), not characters — a
character such as an emoji may take multiple code units. Must be greater than `0`. Defaults to `1000`.

### overlap_size [int]

Overlap length between adjacent chunks, counted in **UTF-16 code units**.
Carrying a little context from the end
of one chunk into the start of the next helps preserve meaning across chunk boundaries. The overlap is
made up of **whole trailing pieces** (complete words/sentences), so it never begins in the middle of a
word; `overlap_size` is therefore an upper bound — the actual overlap rounds down to whole pieces, and
is empty when even the last single piece is longer than the budget. (In the empty-`separators`
fixed-size fallback there are no pieces, so the overlap is a plain `overlap_size`-character window and
is not rounded to a word boundary.) Must satisfy `0 <= overlap_size < chunk_size`. Defaults to `0`.

> Note: the number of chunks grows roughly as `chunk_size / (chunk_size - overlap_size)`, so an
> `overlap_size` close to `chunk_size` multiplies the output row count (and memory) dramatically —
> e.g. `chunk_size = 1000` with `overlap_size = 999` produces about one chunk per character. Keep the
> overlap well below `chunk_size`.

### separators [array]

Separators tried in priority order to avoid cutting mid-sentence: the text is split by the first
separator, and any piece still longer than `chunk_size` is further split by the next separator, and so
on. If the list is left empty, the transform falls back to fixed-size splitting. Defaults to
`["\n\n", "\n", "。", "！", "？", ". ", " "]`.

### skip_empty_text [boolean]

Controls how a row whose `text_field` is `null` or empty is handled. When `true` the row is dropped;
when `false` the row is passed through (`output_field` set to `null`, `chunk_index_field` set to `0`).
Defaults to `true`.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details.

## Behavior

- A `null` or empty text value produces **no** output rows for that input row when `skip_empty_text = true` (the default); set `skip_empty_text = false` to pass such a row through instead (`chunk = null`, `chunk_index = 0`).
- Each produced chunk is at most `chunk_size` UTF-16 code units long (the carried overlap counts toward the budget); a chunk may be up to 1 unit shorter to avoid splitting a surrogate pair.
- Separators are retained: each separator stays attached to the end of the piece it follows, so merging pieces to fill a chunk keeps the spaces/newlines between them (words split on `" "` are re-joined as `a b`, not `ab`) and runs of consecutive separators (e.g. blank lines) are preserved. With `overlap_size = 0` the chunks concatenate back to the original text.
- The rows produced from one input row all carry the same source key (e.g. the same `id`). To keep it unique, the transform appends `chunk_index_field` to the primary key and to every unique key.

## Example

The data read from source is a table like this:

| id |                    content                    |
|----|-----------------------------------------------|
| 1  | SeaTunnel is a data integration platform. ... |

We want to split the `content` field into overlapping chunks, so we can add a `TextChunk` transform
like this:

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

Then each input row is expanded into one row per chunk in result table `fake1`, keeping the original
fields and adding `chunk` / `chunk_index`:

| id |                    content                    |         chunk          | chunk_index |
|----|-----------------------------------------------|------------------------|-------------|
| 1  | SeaTunnel is a data integration platform. ... | SeaTunnel is a data... | 0           |
| 1  | SeaTunnel is a data integration platform. ... | ...platform that ...   | 1           |

## Job Config Example

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

## Changelog

### new version

- Add TextChunk Transform Plugin

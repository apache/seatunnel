# Split

> Split transform plugin

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Description

The Split transform splits one input field into several output fields. The input value is converted with `toString()` and split by `separator`, which is a Java regular expression. The produced columns are always `STRING` columns and are appended after the input columns; the original field is kept. Split supports multi-table configuration, see [Multi-Table Configuration](#multi-table-configuration).

## Key Features

- Split one field into a fixed list of output fields
- `separator` is a Java regular expression, so regex metacharacters must be escaped to match them literally
- A null input value produces null output fields
- Missing parts are padded with null; extra parts remain in the last output field
- Works on multiple tables in one transform, see [Multi-Table Transform](transform-multi-table.md)

## Options

| Name              | Type               | Required | Default       | Description                                                                                          |
|-------------------|--------------------|----------|---------------|------------------------------------------------------------------------------------------------------|
| separator         | String             | yes      | -             | Java regular expression used to split the field.                                                     |
| split_field       | String             | yes      | -             | Input field to split. It must exist in the input table.                                              |
| output_fields     | Array&lt;String&gt; | yes      | -             | Names of the output fields, in order. The list must not be empty.                                    |
| table_match_regex | String             | no       | `.*`          | Regex of the table paths that the transform level rule applies to.                                   |
| table_transform   | Array&lt;Map&gt;    | no       | -             | Per-table rules, see [Multi-Table Transform](transform-multi-table.md).                              |
| rule_match_mode   | String             | no       | `FIRST_MATCH` (duplicate `table_path` entries are rejected unless set explicitly) | How multiple `table_transform` entries with the same `table_path` are evaluated.                     |
| common options    | -                  | no       | -             | Transform plugin common parameters, see [Transform Plugin](common-options/common-options.md).       |

:::info

`separator`, `split_field` and `output_fields` are required in every rule. In a multi-table configuration, the transform level rule can be omitted, but each `table_transform` entry must still contain all three options. Tables without a matching entry are passed through unchanged.

:::

### separator [String]

The Java regular expression passed to `String#split`. There is no default value.

Because the value is a regular expression, metacharacters must be escaped when they should match literally:

- A single space: `" "`
- A comma: `","`
- A literal pipe: `"\\|"`
- A literal dot: `"\\."`
- One or more spaces: `"\\s+"`

:::caution

Using `"|"` without escaping is a common mistake. As a regular expression it is an empty alternative, so the split happens at character boundaries. With `output_fields = [first_name, last_name]`, `Joy|Ding` becomes `J` and `oy|Ding` instead of `Joy` and `Ding`.

:::

### split_field [String]

The name of the input field to split. The value is converted with `toString()` before splitting, so fields of other types are accepted as well.

The field must exist in the input table. Otherwise the job fails with `TRANSFORM_COMMON-01: The input field 'name' of 'Split' transform not found in upstream schema`.

### output_fields [Array&lt;String&gt;]

The names of the fields produced by the transform, in order. The list must not be empty.

Every output column is a nullable `STRING` column with length 200. The columns are appended after the existing input columns; the input field is not removed.

The number of entries also controls how the input value is split:

- If the value produces fewer parts than `output_fields`, the remaining output fields are `null`.
- If the value produces more parts than `output_fields`, the last output field receives the rest of the input, including the remaining separators.

For example, with `separator = " "` and `output_fields = [first_name, last_name]`:

| Input value       | first_name | last_name   |
|-------------------|------------|-------------|
| `Joy Ding`        | Joy        | Ding        |
| `Joy`             | Joy        | `null`      |
| `Joy Ding Junior` | Joy        | Ding Junior |
| `null`            | `null`     | `null`      |

### table_match_regex [String]

A regular expression matched against the upstream table path. The transform level rule (`separator`, `split_field` and `output_fields` outside of `table_transform`) is applied only to the tables that match. The default `.*` matches every table. Tables that match neither this regex nor a `table_transform` entry are passed through unchanged.

Note that this refers to the real upstream table name, not to `plugin_output`.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules. Each entry must contain `table_path` in the form `databaseName[.schemaName].tableName` (matched exactly) and a complete Split rule for that table, which means `separator`, `split_field` and `output_fields`.

When a table has an entry in `table_transform`, the transform level rule is not applied to it. See [Multi-Table Configuration](#multi-table-configuration).

### rule_match_mode [String]

Controls what happens when several `table_transform` entries have the same `table_path`. Supported values are `FIRST_MATCH` (apply only the first matching entry) and `ALL_MATCH` (apply all matching entries in order, each one receiving the output of the previous one).

If `rule_match_mode` is not set, duplicate `table_path` entries are rejected. Set it explicitly to `FIRST_MATCH` or `ALL_MATCH` when several entries use the same `table_path`.

### common options

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details.

## Multi-Table Configuration

When the upstream source outputs several tables (for example JDBC or MySQL-CDC), one Split transform can handle all of them.

- Entries in `table_transform` take precedence over the transform level rule. A table that has an entry is never touched by the transform level rule.
- Every entry must be a complete single-table rule (`table_path` plus `separator`, `split_field` and `output_fields`).
- The transform level rule applies only to tables matching `table_match_regex`.

The configuration below uses a transform level rule for `test.a.*` tables, a per-table rule for `test.xyz`, and leaves `test.www` unchanged:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        row.num = 2
        schema = {
          table = "test.abc"
          fields { id = int, name = string, age = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.abcd"
          fields { id = int, name = string, age = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.xyz"
          fields { id = int, name = string, age = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.www"
          fields { id = int, name = string, age = int }
        }
      }
    ]
  }
}

transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]

    table_transform = [
      {
        table_path = "test.xyz"
        separator = "-"
        split_field = "name"
        output_fields = [part1, part2]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

Result schema of each table:

| Table      | Rule applied                              | Output fields                        |
|------------|-------------------------------------------|--------------------------------------|
| test.abc   | transform level rule (matches `test.a.*`) | id, name, age, first_name, last_name |
| test.abcd  | transform level rule (matches `test.a.*`) | id, name, age, first_name, last_name |
| test.xyz   | `table_transform` entry                   | id, name, age, part1, part2          |
| test.www   | none                                      | id, name, age                        |

## Example

### Single table

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

To split the `name` field into `first_name` and `last_name`, add a `Split` transform:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields { name = string, age = int, card = int }
    }
    rows = [
      { kind = INSERT, fields = ["Joy Ding", 20, 123] },
      { kind = INSERT, fields = ["May Ding", 20, 123] },
      { kind = INSERT, fields = ["Kin Dom", 20, 123] },
      { kind = INSERT, fields = ["Joy Dom", 20, 123] }
    ]
  }
}

transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

The data in the result table `fake1` is:

|   name   | age | card | first_name | last_name |
|----------|-----|------|------------|-----------|
| Joy Ding | 20  | 123  | Joy        | Ding      |
| May Ding | 20  | 123  | May        | Ding      |
| Kin Dom  | 20  | 123  | Kin        | Dom       |
| Joy Dom  | 20  | 123  | Joy        | Dom       |

### Multiple tables

The `env`, `source` and `sink` blocks are the same as in [Multi-Table Configuration](#multi-table-configuration). Only the `transform` block changes. Assume the source outputs `test.abc`, `test.abcd`, `test.xyz` and `test.www`, each with the fields `id`, `name` and `age`.

**1. Transform level rule only.** The rule applies to every table because `table_match_regex` defaults to `.*`.

```hocon
transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]
  }
}
```

**2. Transform level rule plus per-table rules.** See the complete job in [Multi-Table Configuration](#multi-table-configuration).

```hocon
transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_match_regex = "test.a.*"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]
    table_transform = [
      {
        table_path = "test.xyz"
        separator = "-"
        split_field = "name"
        output_fields = [part1, part2]
      }
    ]
  }
}
```

**3. Per-table rules only.** The transform level rule can be omitted; tables without an entry are passed through unchanged.

```hocon
transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_transform = [
      {
        table_path = "test.abc"
        separator = " "
        split_field = "name"
        output_fields = [first_name, last_name]
      },
      {
        table_path = "test.abcd"
        separator = " "
        split_field = "name"
        output_fields = [first_name, last_name]
      },
      {
        table_path = "test.xyz"
        separator = "-"
        split_field = "name"
        output_fields = [part1, part2]
      }
    ]
  }
}
```

`test.www` has no matching entry and is passed through unchanged.

## Changelog

### new version

- Add Split Transform Connector

# Filter

> Filter transform plugin

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Description

The Filter transform decides which fields of a table are passed to the next step. Use `include_fields` to list the fields to keep, or `exclude_fields` to list the fields to remove. ONE of the two must be set. The table name does not change, and the primary key and constraint keys are kept only if all of their columns are still present in the output. Filter supports multi-table configuration, see [Multi-Table Configuration](#multi-table-configuration).

## Key Features

- Keep a list of fields (`include_fields`) or remove a list of fields (`exclude_fields`)
- With `include_fields`, the output fields follow the order of the list
- Primary key and constraint keys are retained when their columns remain
- Works on multiple tables in one transform, see [Multi-Table Transform](transform-multi-table.md)

## Options

| Name              | Type               | Required | Default | Description                                                                                          |
|-------------------|--------------------|----------|---------|------------------------------------------------------------------------------------------------------|
| include_fields    | Array&lt;String&gt;      | no       | -       | Fields to keep. All other fields are removed. Mutually exclusive with `exclude_fields`.              |
| exclude_fields    | Array&lt;String&gt;      | no       | -       | Fields to remove. All other fields are kept. Mutually exclusive with `include_fields`.               |
| table_match_regex | String             | no       | `.*`    | Regex of the table paths that the transform level rule applies to.                                   |
| table_transform   | Array&lt;Map&gt;         | no       | -       | Per-table rules, see [Multi-Table Transform](transform-multi-table.md).                              |
| rule_match_mode   | String             | no       | `FIRST_MATCH` | How multiple `table_transform` entries with the same `table_path` are evaluated.                     |
| common options    | -                  | no       | -       | Transform plugin common parameters, see [Transform Plugin](common-options/common-options.md).       |

:::caution

Exactly one of `include_fields` and `exclude_fields` must be set in every rule. Setting both, or neither, is a configuration error.

:::

### include_fields [Array&lt;String&gt;]

The fields to keep, for example `include_fields = [name, card]` keeps only those two fields. All other fields are removed.

The output fields appear in the order of this list, not in the order of the input table. Every name must exist in the input table, otherwise the job fails with a "cannot find input fields" error. If the option is set, the list must not be empty. It cannot be used together with `exclude_fields`.

:::info

For backward compatibility, the deprecated option name `fields` is still accepted as a fallback for `include_fields`. New configurations should use `include_fields`.

:::

### exclude_fields [Array&lt;String&gt;]

The fields to remove, for example `exclude_fields = [age]` removes `age` and keeps everything else. This is convenient when you only want to delete a few fields from a table with many fields.

The remaining fields keep their original order. Every name must exist in the input table, otherwise the job fails with a "cannot find input fields" error. If the option is set, the list must not be empty. It cannot be used together with `include_fields`.

### table_match_regex [String]

A regular expression matched against the upstream table path. The transform level rule (`include_fields` or `exclude_fields` outside of `table_transform`) is applied only to the tables that match. The default `.*` matches every table. Tables that match neither this regex nor a `table_transform` entry are passed through unchanged.

Note that this refers to the real upstream table name, not to `plugin_output`.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules. Each entry must contain `table_path` in the form `databaseName[.schemaName].tableName` (matched exactly) and a complete Filter rule for that table, which means exactly one of `include_fields` and `exclude_fields`.

When a table has an entry in `table_transform`, the transform level rule is not applied to it. See [Multi-Table Configuration](#multi-table-configuration).

### rule_match_mode [String]

Controls what happens when several `table_transform` entries have the same `table_path`. Supported values are `FIRST_MATCH` (apply only the first matching entry) and `ALL_MATCH` (apply all matching entries in order, each one receiving the output of the previous one). 

If `rule_match_mode` is not set, duplicate `table_path` entries are rejected. Set it explicitly to `FIRST_MATCH` or `ALL_MATCH` when several entries use the same `table_path`.

### common options

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details.

## Multi-Table Configuration

When the upstream source outputs several tables (for example JDBC or MySQL-CDC), one Filter transform can handle all of them.

- Entries in `table_transform` take precedence over the transform level rule. A table that has an entry is never touched by the transform level rule.
- Every entry must be a complete single-table rule (`table_path` plus `include_fields` or `exclude_fields`).
- The transform level rule applies only to tables matching `table_match_regex`.

The configuration below uses a transform level rule for test.a.* tables, a per-table rule for `test.xyz`, and leaves `test.www` unchanged:

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
          fields { id = int, name = string, age = int, card = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.abcd"
          fields { id = int, name = string, age = int, card = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.xyz"
          fields { id = int, name = string, age = int, card = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.www"
          fields { id = int, name = string, age = int, card = int }
        }
      }
    ]
  }
}

transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    include_fields = [name, card]

    table_transform = [
      {
        table_path = "test.xyz"
        exclude_fields = [age]
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

| Table      | Rule applied                                    | Output fields        |
|------------|-------------------------------------------------|----------------------|
| test.abc   | transform level rule (matches `test.a.*`)       | name, card           |
| test.abcd  | transform level rule (matches `test.a.*`)       | name, card           |
| test.xyz   | `table_transform` entry                         | id, name, card       |
| test.www   | none                                            | id, name, age, card  |

## Example

### Single table

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

To keep the fields `name` and `card`, add a `Filter` transform with `include_fields`:

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
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    include_fields = [name, card]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

Or delete the field `age` with `exclude_fields`, which gives the same result:

```hocon
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_fields = [age]
  }
}
```

The data in the result table `fake1` is:

|   name   | card |
|----------|------|
| Joy Ding | 123  |
| May Ding | 123  |
| Kin Dom  | 123  |
| Joy Dom  | 123  |

### Multiple tables

The `env`, `source` and `sink` blocks are the same as in [Multi-Table Configuration](#multi-table-configuration). Only the `transform` block changes. Assume the source outputs `test.abc`, `test.abcd`, `test.xyz` and `test.www`, each with the fields `id`, `name`, `age`, `card`.

**1. Transform level rule only.** The rule applies to every table because `table_match_regex` defaults to `.*`. All four tables become `name, card`.

```hocon
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    include_fields = [name, card]
  }
}
```

**2. Transform level rule plus per-table rules.** See the complete job in [Multi-Table Configuration](#multi-table-configuration).

```hocon
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_match_regex = "test.a.*"
    include_fields = [name, card]
    table_transform = [
      {
        table_path = "test.xyz"
        exclude_fields = [age]
      }
    ]
  }
}
```

## Changelog

### new version

- Add Filter Transform Connector

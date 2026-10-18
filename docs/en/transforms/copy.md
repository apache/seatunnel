# Copy

> Copy transform plugin

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The Copy transform adds new fields by copying values from existing fields. The original fields remain in the output. Use it when downstream processing needs another field name while retaining the source field. In a multi-table job, you can set one transform-level rule and override it for individual tables.

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `fields` | `Map<String, String>` | Conditional | - | Maps each new field name to an existing source field name. |
| `src_field` | `String` | Conditional | - | Deprecated source field option; use with `dest_field`. |
| `dest_field` | `String` | Conditional | - | Deprecated destination field option; required with `src_field`. |
| `table_transform` | `List<Map<String, Object>>` | No | `[]` | Complete Copy rules for specific tables. |
| `table_transform.table_path` | `String` | Yes, in each entry | - | Exact path of the table selected by an entry. |
| `table_match_regex` | `String` | No | `.*` | Regex selecting tables for the transform-level rule. |
| `rule_match_mode` | `String` | No | `FIRST_MATCH` | How multiple entries for the same table are applied. |
| `common options` | - | No | - | See [Transform Plugin](common-options/common-options.md). |

### fields [Map<String, String>]

Each entry maps a **new field name** to an **existing source field name**. For example, `name_copy = name` adds `name_copy` with the value of `name`. The map must not be empty, and every source field must exist in the input table. A Copy rule must use either `fields` or the deprecated `src_field` and `dest_field` pair; do not use both forms in the same rule.

```hocon
fields {
  name_copy = name
  age_copy = age
}
```

### src_field [String]

Deprecated single-field form. Set it together with `dest_field`; use `fields` for new configurations. It must not be blank or appear in the same rule as `fields`.

### dest_field [String]

Deprecated destination field for `src_field`. It is required and must not be blank when `src_field` is set. For example, `src_field = "name"` and `dest_field = "name_copy"` copy `name` to `name_copy`.

### table_transform [List<Map<String, Object>>]

Each entry selects a table with `table_path` and contains a complete Copy rule: either a non-empty `fields` map or the deprecated `src_field` and `dest_field` pair. An entry takes precedence over the transform-level rule for its table. If only `table_transform` is configured, tables without an entry pass through unchanged. See [Multi-Table Transform](transform-multi-table.md).

### table_transform.table_path [String]

The non-blank path of an upstream table, such as `test.users`. It is matched exactly against the table path, not against `plugin_output`.

### table_match_regex [String]

A regular expression selecting tables for the transform-level Copy rule. Its default, `.*`, matches all table paths. An exact `table_transform` entry still takes precedence for its table.

### rule_match_mode [String]

Accepts `FIRST_MATCH` and `ALL_MATCH`; the default is `FIRST_MATCH`. With `FIRST_MATCH`, the first entry for a table is used. With `ALL_MATCH`, matching entries run in order, so a later entry can copy a field created by an earlier entry. Duplicate exact `table_path` entries are rejected when this option is not explicitly configured.

### common options

For shared transform settings such as `plugin_input` and `plugin_output`, see [Transform Plugin](common-options/common-options.md).

## Multi-Table Configuration

The following complete job generates two tables. The entry for `test.users` overrides the outer `fields` rule; `test.orders` uses that outer rule. `table_transform` entries take precedence even when the table also matches `table_match_regex`.

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
        schema = {
          table = "test.users"
          fields {
            id = int
            name = string
          }
        }
        rows = [
          { kind = INSERT, fields = [1, "Alice"] }
        ]
      },
      {
        schema = {
          table = "test.orders"
          fields {
            id = int
            name = string
          }
        }
        rows = [
          { kind = INSERT, fields = [2, "Bob"] }
        ]
      }
    ]
  }
}

transform {
  Copy {
    plugin_input = "fake"
    plugin_output = "copied"

    fields {
      name_copy = name
    }

    table_transform = [
      {
        table_path = "test.users"
        fields {
          user_name_copy = name
        }
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "copied"
  }
}
```

## Example

For the complete job above, the input and expected output are:

| Table | Input fields and values | Output fields and values |
|-------|-------------------------|--------------------------|
| `test.users` | `id=1, name=Alice` | `id=1, name=Alice, user_name_copy=Alice` |
| `test.orders` | `id=2, name=Bob` | `id=2, name=Bob, name_copy=Bob` |

The complete job also demonstrates how to change between three configurations:

- **Transform-level rule only:** remove `table_transform`. Both tables receive `name_copy`.
- **Transform-level plus per-table rule:** use the complete job above. `test.users` receives `user_name_copy`; `test.orders` receives `name_copy`.
- **Per-table rules only:** remove the outer `fields` block. `test.users` receives `user_name_copy`; `test.orders` passes through unchanged.

## Changelog

### new version

- Add Copy Transform Connector
- Support copying fields to new fields

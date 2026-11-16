# FilterRowKind

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark

## Description

FilterRowKind keeps or drops rows according to their change kind. Use it to select events such as inserts or deletes from an upstream changelog. It does not convert one row kind into another: retained rows keep their values, schema and row kind. Filtering out a row removes that event from the downstream stream.

:::caution
Dropping update or delete events changes the changelog received by the sink. Choose a rule that matches the downstream use case; filtering is not a substitute for applying a complete CDC changelog.
:::

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| include_kinds | Array&lt;Enum&gt; | Conditional | - | Keep only the listed row kinds. Mutually exclusive with `exclude_kinds`; the list must not be empty. |
| exclude_kinds | Array&lt;Enum&gt; | Conditional | - | Drop the listed row kinds. Mutually exclusive with `include_kinds`; the list must not be empty. |
| table_match_regex | String | No | `.*` | Match upstream table paths to which the transform-level rule applies. |
| table_transform | Array&lt;Map&gt; | No | `[]` | Complete rules for individual tables, selected by an exact `table_path`. |
| rule_match_mode | Enum | No | `FIRST_MATCH` | Select the first matching per-table rule or apply all matching rules in order. See the duplicate-path behavior below. |
| common options | - | No | - | Common transform options such as `plugin_input` and `plugin_output`. |

### include_kinds [Array&lt;Enum&gt;]

Keep a row only when its row kind is in this list, for example `include_kinds = ["INSERT", "UPDATE_AFTER"]`. Accepted values are:

| Value | Meaning |
|-------|---------|
| `INSERT` | An inserted row. |
| `UPDATE_BEFORE` | The previous contents of an updated row. |
| `UPDATE_AFTER` | The new contents of an updated row. |
| `DELETE` | A deleted row. |

Without a nonempty `table_transform` list, configure exactly one of `include_kinds` and `exclude_kinds` at the transform level. Configuring both, neither, or an empty selected list fails validation. An empty `table_transform = []` does not remove this requirement.

With per-table entries, the transform-level include/exclude rule may be absent. If supplied, it must still contain exactly one nonempty list. Each per-table entry must independently satisfy the same rule.

### exclude_kinds [Array&lt;Enum&gt;]

Drop rows whose row kind is in this list and keep all other rows. It accepts the same four values as `include_kinds` and has no default. For example, `exclude_kinds = ["DELETE", "UPDATE_BEFORE"]` keeps inserts and after-update rows.

Do not configure both options in the same rule. A per-table rule can use a different option from the transform-level rule because it replaces that rule for its table.

### table_match_regex [String]

A Java regular expression matched against the whole upstream table path, such as `test.abc` or `database.schema.table`. The default `.*` matches all table paths. This is the actual table path, not the dataset name in `plugin_input` or `plugin_output`.

This option selects tables for the transform-level rule. It does not restrict explicit `table_transform` entries, and it does not create a filtering rule when the transform-level include/exclude options are absent.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules; the default is an empty list. Each entry must have a nonblank `table_path` and exactly one nonempty `include_kinds` or `exclude_kinds` list. `table_path` is an exact path, not a regular expression.

An entry is a complete rule and does not inherit the transform-level include/exclude list. A matching entry takes precedence even when its table does not match `table_match_regex`.

### rule_match_mode [Enum]

Controls multiple entries with the same exact `table_path`:

- Explicit `FIRST_MATCH`: use the first matching entry in list order.
- Explicit `ALL_MATCH`: apply all matching entries in list order. A row must survive every filter; this is not a union of the retained kinds.

Although the option's default is `FIRST_MATCH`, omitting the option rejects duplicate `table_path` entries during transform initialization. Set it explicitly if repeated paths are intentional. With unique paths, the default is sufficient.

### common options

Use `plugin_input` and `plugin_output` to connect this transform to upstream and downstream plugins. See [Transform Plugin](common-options/common-options.md) for common parameters.

## Multi-Table Configuration

For each input table, the effective rule is selected in this order:

1. Use the matching `table_transform` entry or entries, according to `rule_match_mode`.
2. Otherwise, apply the transform-level rule if one is configured and the table path matches `table_match_regex`.
3. Otherwise, pass the table's rows through unchanged.

In a `table_transform`-only configuration, unlisted tables pass through unchanged. FilterRowKind filters rows, not catalog table definitions; a table can remain in the output schema even when all its rows are dropped.

The following examples cover all three configuration shapes. See [Multi-Table Transform](transform-multi-table.md) for the shared matching rules.

## Examples

These complete batch configurations use FakeSource and Console. Install those connectors in your SeaTunnel distribution before running a saved configuration with `bin/seatunnel.sh --config <file> -e local`. The generated rows in these examples have kind `INSERT`; their random field values do not affect the expected row counts.

### Transform-level rule only

Input: three `INSERT` rows from `test.abc`. Output: zero rows, because the rule excludes `INSERT`.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    row.num = 3
    schema {
      table = "test.abc"
      fields { id = "int" }
    }
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "filtered"
    exclude_kinds = ["INSERT"]
  }
}

sink {
  Console {
    plugin_input = "filtered"
  }
}
```

### Transform-level rule with per-table rules

Each input table contains three `INSERT` rows. The explicit entry for `test.xyz` overrides the transform-level exclusion. `test.www` does not match the regex and passes through unchanged.

| Table | Input rows | Output rows | Effective rule |
|-------|------------|-------------|----------------|
| `test.abc` | 3 | 0 | Transform-level `exclude_kinds = ["INSERT"]`. |
| `test.xyz` | 3 | 3 | Per-table `include_kinds = ["INSERT"]`. |
| `test.www` | 3 | 3 | No matching rule; unchanged. |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    tables_configs = [
      {
        row.num = 3
        schema {
          table = "test.abc"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.xyz"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.www"
          columns = [{ name = "id", type = "int" }]
        }
      }
    ]
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "filtered"
    table_match_regex = "test[.](abc|xyz)"
    exclude_kinds = ["INSERT"]
    table_transform = [
      {
        table_path = "test.xyz"
        include_kinds = ["INSERT"]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "filtered"
  }
}
```

### Per-table rules only

There is no transform-level include/exclude rule. Only `test.xyz` drops its inserts; the other tables pass through unchanged.

| Table | Input rows | Output rows |
|-------|------------|-------------|
| `test.abc` | 3 | 3 |
| `test.xyz` | 3 | 0 |
| `test.www` | 3 | 3 |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    tables_configs = [
      {
        row.num = 3
        schema {
          table = "test.abc"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.xyz"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.www"
          columns = [{ name = "id", type = "int" }]
        }
      }
    ]
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "filtered"
    table_transform = [
      {
        table_path = "test.xyz"
        exclude_kinds = ["INSERT"]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "filtered"
  }
}
```


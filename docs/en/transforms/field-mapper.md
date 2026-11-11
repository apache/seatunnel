# FieldMapper

> FieldMapper transform plugin

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

FieldMapper selects input fields and maps their names to output field names. Use it to rename or reorder fields and remove fields that a downstream sink does not need. Only fields included in `field_mapper` remain in the output; their values and data types are preserved. The table path does not change. For sources that produce multiple tables, you can configure a shared mapping and override it with complete per-table mappings.

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| field_mapper | Map&lt;String, String&gt; | conditionally yes | - | Non-empty mapping from input field names to output field names. Required at the transform level when `table_transform` is absent or empty, and in every `table_transform` entry. |
| table_match_regex | String | no | `.*` | Regular expression selecting the table paths to which the transform-level mapping applies. |
| table_transform | Array&lt;Map&gt; | no | `[]` | Complete mappings for individual tables, selected by exact `table_path`. |
| rule_match_mode | String | no | `FIRST_MATCH` | How multiple entries with the same `table_path` are applied. Must be set explicitly to allow duplicate paths. |
| common options | - | no | - | Dataset wiring options; see [Transform Common Options](common-options/common-options.md). |

### field_mapper [Map&lt;String, String&gt;]

Each key is an existing input field name, and its value is the output field name. For example, `name = display_name` renames `name` to `display_name`, while `id = id` retains `id` with its original name. Fields not listed as keys are removed. This option changes field selection and names, not field values or types.

The mapping must not be empty. Every key must exist in the input schema of the table receiving this rule; a missing field causes transform initialization to fail. When `table_transform` contains entries, the transform-level `field_mapper` may be omitted. If it is supplied, it must still be a valid non-empty mapping.

Primary keys and constraint keys are retained, with their column names mapped, only when all of their columns remain in the output.

Output columns follow the mapping order, rather than the input schema order. For example, `field_mapper = { name = display_name, id = id }` produces `display_name` followed by `id`, even when the input schema starts with `id`.

### table_match_regex [String]

A regular expression matched against the entire upstream table path. The default `.*` matches every table. For example, `demo[.](users|admins)` matches `demo.users` and `demo.admins`.

This option selects tables for the transform-level `field_mapper` only. It does not restrict explicit `table_transform` entries. It matches actual table paths, not dataset names such as `plugin_input` or `plugin_output`. If there is no transform-level mapping, this option does not create one.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules. Each entry must contain:

- `table_path`: a non-blank string exactly matching the upstream table path, for example `demo.admins`.
- `field_mapper`: a complete, non-empty mapping for that table.

An entry takes precedence over the transform-level mapping and does not inherit its fields. In particular, a field retained by the transform-level mapping is still removed from an explicitly matched table if its per-table mapping omits that field. The default is an empty list.

### rule_match_mode [String]

Controls entries in `table_transform` that have the same `table_path`:

- `FIRST_MATCH`: apply the first matching entry only.
- `ALL_MATCH`: apply all matching entries in list order. Each mapping receives the schema and rows produced by the previous mapping, so its keys must refer to fields that still exist under their current names.

Although the default is `FIRST_MATCH`, omitting this option rejects duplicate `table_path` entries. Set `rule_match_mode` explicitly to `FIRST_MATCH` or `ALL_MATCH` when duplicate paths are intentional.

### common options

Use `plugin_input` and `plugin_output` to connect the transform to named datasets. See [Transform Common Options](common-options/common-options.md) for wiring behavior and deprecated option names. These dataset names do not rename the tables inside the dataset.

## Multi-Table Configuration

For each upstream table, FieldMapper resolves the mapping in this order:

1. If there is an exact `table_transform` entry, apply the selected per-table rule or rules.
2. Otherwise, if a transform-level `field_mapper` exists and the table path matches `table_match_regex`, apply that mapping.
3. Otherwise, pass the table through with its schema and rows unchanged.

When only `table_transform` is configured, tables without an entry remain unchanged. Every entry is validated independently, even when a transform-level mapping is also present. See [Multi-Table Transform](transform-multi-table.md) for the shared behavior.

## Example

The following complete batch configurations use FakeSource with explicit rows and Console output. They require the FakeSource and Console connectors. Column values in the result tables are identified by name; Console log formatting may differ.

### Transform-level mapping only

Input table `demo.users`:

| id | name | age |
|----|------|-----|
| 1 | Alice | 20 |

Keep `id`, rename `name` to `display_name`, and remove `age`:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "input"
    schema {
      table = "demo.users"
      columns = [
        { name = "id", type = "int" },
        { name = "name", type = "string" },
        { name = "age", type = "int" }
      ]
    }
    rows = [{ kind = INSERT, fields = [1, "Alice", 20] }]
  }
}

transform {
  FieldMapper {
    plugin_input = "input"
    plugin_output = "mapped"
    field_mapper = {
      id = id
      name = display_name
    }
  }
}

sink {
  Console {
    plugin_input = "mapped"
  }
}
```

Output table `demo.users`:

| id | display_name |
|----|--------------|
| 1 | Alice |

### Transform-level mapping with a per-table override

The input dataset contains three tables, each with the schema `id INT, name STRING, age INT`:

| Table path | id | name | age |
|------------|----|------|-----|
| demo.users | 1 | Alice | 20 |
| demo.admins | 2 | Bob | 30 |
| demo.audit | 3 | Carol | 40 |

The regex matches both `demo.users` and `demo.admins`, but the explicit mapping for `demo.admins` takes precedence. `demo.audit` matches neither rule and remains unchanged.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "input"
    tables_configs = [
      {
        schema {
          table = "demo.users"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [1, "Alice", 20] }]
      },
      {
        schema {
          table = "demo.admins"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [2, "Bob", 30] }]
      },
      {
        schema {
          table = "demo.audit"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [3, "Carol", 40] }]
      }
    ]
  }
}

transform {
  FieldMapper {
    plugin_input = "input"
    plugin_output = "mapped"
    table_match_regex = "demo[.](users|admins)"
    field_mapper = {
      id = id
      name = display_name
    }
    table_transform = [
      {
        table_path = "demo.admins"
        field_mapper = {
          id = admin_id
          name = admin_name
        }
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "mapped"
  }
}
```

Expected output, shown as field-name/value pairs:

| Table path | Output fields and values |
|------------|--------------------------|
| demo.users | `id = 1`, `display_name = "Alice"` |
| demo.admins | `admin_id = 2`, `admin_name = "Bob"` |
| demo.audit | `id = 3`, `name = "Carol"`, `age = 40` |

### Per-table mapping only

This configuration uses the same input tables and rows as the preceding example. It omits the transform-level mapping, so only `demo.admins` is transformed. Both `demo.users` and `demo.audit` remain unchanged.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "input"
    tables_configs = [
      {
        schema {
          table = "demo.users"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [1, "Alice", 20] }]
      },
      {
        schema {
          table = "demo.admins"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [2, "Bob", 30] }]
      },
      {
        schema {
          table = "demo.audit"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [3, "Carol", 40] }]
      }
    ]
  }
}

transform {
  FieldMapper {
    plugin_input = "input"
    plugin_output = "mapped"
    table_transform = [
      {
        table_path = "demo.admins"
        field_mapper = {
          id = admin_id
          name = admin_name
        }
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "mapped"
  }
}
```

Expected output:

| Table path | Output fields and values |
|------------|--------------------------|
| demo.users | `id = 1`, `name = "Alice"`, `age = 20` |
| demo.admins | `admin_id = 2`, `admin_name = "Bob"` |
| demo.audit | `id = 3`, `name = "Carol"`, `age = 40` |

## Changelog

### new version

- Add FieldMapper Transform Connector

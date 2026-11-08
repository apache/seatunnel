# FieldRename

> FieldRename transform plugin

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Description

The FieldRename transform renames the columns of a table without changing the table name or the row data. It can convert the case of field names, add a prefix or suffix, apply regex or exact-match replacement rules, and rename specific fields explicitly. Primary key and constraint key columns are renamed together with their fields. FieldRename supports multi-table configuration, see [Multi-Table Configuration](#multi-table-configuration).

## Key Features

- Convert every field name to `UPPER` or `LOWER` case
- Add a prefix and/or a suffix to every field name
- Replace every match of a regex, or replace a field name by exact match
- Rename specific fields explicitly; `specific` rules take precedence over all other rename options
- Rename primary key and constraint key columns together with their fields
- Handle multiple tables in one transform, see [Multi-Table Transform](transform-multi-table.md)

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| convert_case | Enum | no | - | Case conversion for field names. Supported values: `UPPER`, `LOWER`. |
| prefix | String | no | - | Prefix added to every field name. |
| suffix | String | no | - | Suffix added to every field name. |
| replacements_with_regex | Array&lt;Map&gt; | no | - | Replacement rules. Each entry contains `replace_from`, `replace_to`, and optional `is_regex` (default `true`). |
| specific | Array&lt;Map&gt; | no | - | Explicit rename rules. Each entry contains `field_name` and `target_name`. A matching rule wins over all other rename options. |
| table_match_regex | String | no | `.*` | Regex of the table paths that the transform level rule applies to. |
| table_transform | Array&lt;Map&gt; | no | - | Per-table rules, see [Multi-Table Transform](transform-multi-table.md). |
| rule_match_mode | String | no | `FIRST_MATCH` | How multiple `table_transform` entries with the same `table_path` are evaluated. |
| common options | - | no | - | Transform plugin common parameters, see [Transform Plugin](common-options/common-options.md). |

### convert_case [Enum]

Converts every field name to uppercase (`UPPER`) or lowercase (`LOWER`). When it is not set, the case of the field name is unchanged.

### prefix [String]

Adds a prefix to every output field name. The prefix is added after `convert_case` and `replacements_with_regex` are applied. Leading and trailing whitespace in the value is ignored.

### suffix [String]

Adds a suffix to every output field name. The suffix is added after `convert_case` and `replacements_with_regex` are applied. Leading and trailing whitespace in the value is ignored.

### replacements_with_regex [Array&lt;Map&gt;]

A list of replacement rules. Each entry supports the following fields:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| replace_from | String | yes | - | A Java regular expression, or an exact field name when `is_regex` is `false`. |
| replace_to | String | yes | - | The replacement text. Every match of `replace_from` is replaced with this text. |
| is_regex | Boolean | no | `true` | When `true`, `replace_from` is a regular expression. When `false`, `replace_from` must match the entire field name. |

Every match of `replace_from` in the field name is replaced with `replace_to`. Matching is performed against the original field name, before `convert_case` is applied. If several rules match the same field name, the last matching rule in the list is used. Leading and trailing whitespace in `replace_to` is ignored.

:::caution

`replace_from` and `replace_to` must be set on every entry. An entry with an empty `replace_from` is ignored. If an entry matches a field but `replace_to` is missing, the job fails during initialization.

:::

### specific [Array&lt;Map&gt;]

A list of explicit rename rules. Each entry contains `field_name` and `target_name`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| field_name | String | yes | The input field name to match. The comparison is case-sensitive. |
| target_name | String | yes | The output field name. |

When a field matches a rule, it is renamed to `target_name` and `convert_case`, `replacements_with_regex`, `prefix`, and `suffix` are not applied to that field. If several rules match the same field name, the first one is used.

### table_match_regex [String]

A regular expression matched against the upstream table path. The transform level rule (the options outside `table_transform`) is applied only to the tables that match. The default `.*` matches every table. Tables that match neither this regex nor a `table_transform` entry are passed through unchanged.

Note that this refers to the real upstream table name, not to `plugin_output`.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules. Each entry must contain `table_path` in the form `databaseName[.schemaName].tableName` (matched exactly), plus the FieldRename options to apply to that table.

When a table has an entry in `table_transform`, the transform level rule is not applied to it. See [Multi-Table Configuration](#multi-table-configuration).

### rule_match_mode [String]

Controls what happens when several `table_transform` entries have the same `table_path`. Supported values are `FIRST_MATCH` (apply only the first matching entry) and `ALL_MATCH` (apply all matching entries in order, each one receiving the output of the previous one).

If `rule_match_mode` is not set, duplicate `table_path` entries are rejected. Set it explicitly to `FIRST_MATCH` or `ALL_MATCH` when several entries use the same `table_path`.

### common options

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details.

## Multi-Table Configuration

When the upstream source outputs several tables (for example JDBC or MySQL-CDC), one FieldRename transform can handle all of them.

- Entries in `table_transform` take precedence over the transform level rule. A table that has an entry is never touched by the transform level rule.
- Every entry must contain `table_path` and the FieldRename options for that table.
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
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    convert_case = "UPPER"
    prefix = "F_"

    table_transform = [
      {
        table_path = "test.xyz"
        convert_case = "LOWER"
        suffix = "_S"
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

| Table | Rule applied | Output fields |
|-------|--------------|---------------|
| test.abc | transform level rule (matches `test.a.*`) | F_ID, F_NAME, F_AGE |
| test.abcd | transform level rule (matches `test.a.*`) | F_ID, F_NAME, F_AGE |
| test.xyz | `table_transform` entry | id_s, name_s, age_s |
| test.www | none | id, name, age |

## Example

### Single table

The data read from source is a table like this:

| InvoiceNum | VendorID |
|------------|----------|
| 1001 | Vendor A |
| 1002 | Vendor B |

Convert camelCase field names to snake_case:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      columns = [
        { name = "InvoiceNum", type = "bigint" },
        { name = "VendorID", type = "string" }
      ]
    }
    rows = [
      { kind = INSERT, fields = [1001, "Vendor A"] },
      { kind = INSERT, fields = [1002, "Vendor B"] }
    ]
  }
}

transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"

    convert_case = "LOWER"
    replacements_with_regex = [
      {
        replace_from = "(?<=[a-z0-9])(?=[A-Z])"
        replace_to = "_"
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

The data in the result table `fake1` is:

| invoice_num | vendor_id |
|-------------|-----------|
| 1001 | Vendor A |
| 1002 | Vendor B |

The regex matches the position between a lowercase letter or digit and an uppercase letter. The underscore is inserted at that position, so `InvoiceNum` becomes `invoice_num` and `VendorID` becomes `vendor_id`.

### Add a prefix and suffix

```hocon
transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"
    prefix = "f_"
    suffix = "_s"
  }
}
```

The fields become `f_InvoiceNum_s` and `f_VendorID_s`.

### Rename specific fields

```hocon
transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"
    specific = [
      { field_name = "InvoiceNum", target_name = "invoice_number" }
    ]
  }
}
```

`InvoiceNum` becomes `invoice_number` and `VendorID` is unchanged. Because `specific` takes precedence, any `convert_case`, `replacements_with_regex`, `prefix`, or `suffix` option is ignored for a matching field.

### Replace a field name by exact match

```hocon
transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"
    replacements_with_regex = [
      {
        replace_from = "VendorID"
        replace_to = "vendor_id"
        is_regex = false
      }
    ]
  }
}
```

`VendorID` becomes `vendor_id`; `InvoiceNum` is unchanged. With `is_regex = false`, only a field whose full name equals `replace_from` is replaced.

### Multiple tables

See [Multi-Table Configuration](#multi-table-configuration).

## Changelog

### new version

- Add FieldRename Transform Connector

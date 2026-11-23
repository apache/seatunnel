# Replace

> Replace transform plugin

## Description

Examines string value in given fields and replaces substring of the string value that matches the given string literal or regexes with the given replacement.

## Options

|     name      | type    | required | default value |
|---------------|---------|----------|---------------|
| replace_fields | array   | yes      |               |
| pattern       | string  | yes      | -             |
| replacement   | string  | yes      | -             |
| is_regex      | boolean | no       | false         |
| replace_first | boolean | no       | false         |
| table_match_regex | string  | no       | `.*`          |
| table_transform | array   | no       | -             |
| rule_match_mode | string  | no       | `FIRST_MATCH` |

### replace_fields [array]

The fields you want to replace

`replace_fields` accepts a list such as `["name"]` or `["name", "title"]`.

For backward compatibility, legacy key `replace_field = "name"` is still supported, but new configurations should use `replace_fields`.

Field names are resolved again when the input schema is refreshed, including checkpoint schema restoration. Adding, removing, or reordering other columns therefore does not change which fields receive replacements. If a configured field is absent from the refreshed schema (for example, it was dropped or renamed), the refresh fails with a missing-field error before subsequent rows are processed. Update `replace_fields` to match the source schema before restarting the job.

This field-binding behavior does not change the engine's schema-evolution support or the existing value-conversion and output-schema behavior of Replace.

### pattern [string]

The old string that will be replaced

### replacement [string]

The new string for replace

### is_regex [boolean]

Use regex for string match

### replace_first [boolean]

Whether replace the first match string. Only used when `is_regex = true`.

### table_match_regex [string]

Regex matched against the upstream table path. The transform level rule (the options above outside of `table_transform`) only applies to tables whose path matches this regex. The default `.*` matches every table.

### table_transform [array]

A list of per-table rules. Each entry must contain `table_path` in the form `databaseName[.schemaName].tableName` (matched exactly) and a complete Replace rule for that table (`replace_fields`, `pattern`, `replacement`). Tables with an entry are not affected by the transform level rule.

### rule_match_mode [string]

Controls what happens when several `table_transform` entries have the same `table_path`: `FIRST_MATCH` applies only the first matching entry, `ALL_MATCH` applies all matching entries in order. If not set, duplicate `table_path` entries are rejected.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details

## Example

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

We want to replace the char ` ` to `_` at the `name` field. Then we can add a `Replace` Transform like this:

```
transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_fields = ["name"]
    pattern = " "
    replacement = "_"
    is_regex = true
  }
}
```

Then the data in result table `fake1` will update to

|   name   | age | card |
|----------|-----|------|
| Joy_Ding | 20  | 123  |
| May_Ding | 20  | 123  |
| Kin_Dom  | 20  | 123  |
| Joy_Dom  | 20  | 123  |

## Job Config Example

```
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
      }
    }
  }
}

transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_fields = ["name"]
    pattern = ".+"
    replacement = "b"
    is_regex = true
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

- Add Replace Transform Connector

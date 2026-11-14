# TableFilter

> TableFilter transform plugin

## Description

TableFilter transform plugin for filter tables.

## Options

|       name       | type   | required | default value | Description                                                                                                                                                           |
|:----------------:|--------|----------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| database_pattern | string | no       |               | Specify database filter pattern, the default value is null, which means no filtering. If you want to filter the database name, please set it to a regular expression. |
|  schema_pattern  | string | no       |               | Specify schema filter pattern, the default value is null, which means no filtering. If you want to filter the schema name, please set it to a regular expression.     |
|  table_pattern   | string | no       |               | Specify table filter pattern, the default value is null, which means no filtering. If you want to filter the table name, please set it to a regular expression.       |
|   pattern_mode   | enum   | no       | INCLUDE       | Specify pattern mode, the default value is INCLUDE, which means include the matched table. If you want to exclude the matched table, please set it to EXCLUDE.        |
| table_match_regex | string | no       | `.*`          | Regex matched against the upstream table path. The transform level rule only applies to matching tables.                                                               |
| table_transform   | array  | no       | -             | Per-table rules, see [Multi-Table Transform](transform-multi-table.md). Each entry holds the filter patterns of a single table (`table_path` plus the options above).  |
| rule_match_mode   | string | no       | `FIRST_MATCH` | How multiple `table_transform` entries with the same `table_path` are evaluated.                                                                                       |

## Examples

### Include filter tables

Include filter tables with the name matching the regular expression `user_\d+` in the database `test`.

```hocon
transform {
    TableFilter {
        plugin_input = "source1"
        plugin_output = "transform_a_1"
    
        database_pattern = "test"
        table_pattern = "user_\\d+"
    }
}
```

### Exclude filter tables

Exclude filter tables with the name matching the regular expression `user_\d+` in the database `test`.

```hocon
transform {
    TableFilter {
        plugin_input = "source1"
        plugin_output = "transform_a_1"
    
        database_pattern = "test"
        table_pattern = "user_\\d+"
        pattern_mode = "EXCLUDE"
    }
}
```
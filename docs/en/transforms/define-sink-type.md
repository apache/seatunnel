# Define Sink Type

> Define sink type transform plugin

## Description

Used to define the storage type of sink field. This is effective when the savemode enables automatic table creation.

## Options

|  name   | type                      | required | default value | Description                                                            |
|:-------:|---------------------------|----------|---------------|------------------------------------------------------------------------|
| columns | list<map<string, string>> | yes      |               | The columns to be defined, the name and type of the column must be set |
| table_match_regex | string | no | `.*` | Regex matched against the upstream table path. The transform level rule only applies to matching tables. |
| table_transform | array | no | - | Per-table rules, see [Multi-Table Transform](transform-multi-table.md). Each entry contains `table_path` and the `columns` of that table. |
| rule_match_mode | string | no | `FIRST_MATCH` | How multiple `table_transform` entries with the same `table_path` are evaluated. |

## Examples

### Define sink columns type for savemode

```
transform {
  DefineSinkType {
    columns = [
        {
            column = "c1"
            type = "nvarchar2(10)"
        }
        {
            column = "c2"
            type = "datetime(6)"
        }
        {
            column = "c3"
            type = "your target type"
        }
    ]
  }
}
```

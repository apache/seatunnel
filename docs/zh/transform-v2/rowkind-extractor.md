# RowKindExtractor

> RowKindExtractor transform plugin

## Description

将CDC Row 转换为 Append only Row, 转换后的行扩展了RowKind字段 <br />
Example: <br />
CDC row: -D 1, test1, test2 <br />
transformed Row: +I 1,test1,test2,DELETE

## Options

| name              | type   | required | default value |
|-------------------|--------|----------|---------------|
| custom_field_name | string | yes      | row_kind      |
| transform_type    | enum   | yes      | SHORT         |

### custom_field_name [string]

RowKind列的自定义名

### transform_type [enum]

格式化RowKind值 , 配置为 `SHORT` 或 `FULL`

`SHORT` : +I, -U , +U, -D
`FULL` : INSERT, UPDATE_BEFORE, UPDATE_AFTER , DELETE

## Examples

```yaml

env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
    FakeSource {
        schema = {
            fields {
                pk_id = bigint
                name = string
                score = int
            }
            primaryKey {
                name = "pk_id"
                columnNames = [pk_id]
            }
        }
        rows = [
            {
                kind = INSERT
                fields = [1, "A", 100]
            },
            {
                kind = INSERT
                fields = [2, "B", 100]
            },
            {
                kind = INSERT
                fields = [3, "C", 100]
            },
            {
                kind = INSERT
                fields = [4, "D", 100]
            },
            {
                kind = UPDATE_BEFORE
                fields = [1, "A", 100]
            },
            {
                kind = UPDATE_AFTER
                fields = [1, "F", 100]
            }
            {
                kind = UPDATE_BEFORE
                fields = [2, "B", 100]
            },
            {
                kind = UPDATE_AFTER
                fields = [2, "G", 100]
            },
            {
                kind = DELETE
                fields = [3, "C", 100]
            },
            {
                kind = DELETE
                fields = [4, "D", 100]
            }
        ]
    }
}

transform {
  RowKindExtractor {
        custom_field_name = "custom_name"
        transform_type = FULL
        plugin_output = "trans_result"
    }
}

sink {
  Console {
    plugin_input = "custom_name"
  }
}

```


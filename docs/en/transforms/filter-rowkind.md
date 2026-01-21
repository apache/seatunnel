# FilterRowKind

> FilterRowKind transform plugin

## Description

Filter the data by RowKind

## Options

|     name      | type  | required | default value |
|---------------|-------|----------|---------------|
| include_kinds | array | yes      |               |
| exclude_kinds | array | yes      |               |

### include_kinds [array]

The row kinds to include

### exclude_kinds [array]

The row kinds to exclude.

You can only config one of `include_kinds` and `exclude_kinds`.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Examples

The RowKink of the data generate by FakeSource is `INSERT`, If we use `FilterRowKink` transform and exclude the `INSERT` data, we will write zero rows into sink.

```yaml

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
        age = "int"
      }
    }
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_kinds = ["INSERT"]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```


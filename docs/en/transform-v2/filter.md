# Filter

> Filter transform plugin

## Description

Filter the field.

## Options

|  name  | type  | required | default value |
|--------|-------|----------|---------------|
| fields | array | yes      |               |

### fields [array]

The list of fields that need to be kept. Fields not in the list will be deleted

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

We want to delete field `age`, we can add `Filter` Transform like this

```
transform {
  Filter {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [name, card]
  }
}
```

Then the data in result table `fake1` will like this

|   name   | card |
|----------|------|
| Joy Ding | 123  |
| May Ding | 123  |
| Kin Dom  | 123  |
| Joy Dom  | 123  |

## Changelog

### new version

- Add Filter Transform Connector


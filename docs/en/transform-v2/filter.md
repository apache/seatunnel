# Filter

> Filter transform plugin

## Description

Filter the field.

## Options

|  name  |  type  | required | default value |
|--------|--------|----------|---------------|
| fields | array  | yes      |               |
| mode   | String | no       | KEEP          |

### fields [array]

The list of fields that need to be processed. The fields in the list are either kept or deleted according to the `mode` parameter.

### mode [string]

The working mode of this filter, the default value is `KEEP`, which means that only the fields in the `fields` list are kept, and the fields not in the list are deleted. If the value is `DELETE`, the fields in the `fields` list are deleted, and the fields not in the list are kept.

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

we want to keep the field named `name`, `card`, we can add a `Filter` Transform like below:

```
transform {
  Filter {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [name, card]
  }
}
```

Or we can delete the field named `age` by adding a `Filter` Transform with the mode field set to `DELETE` like below:

```
transform {
  Filter {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [age]
    mode = "DELETE"
  }
}
```

It is useful when you want to delete a small number of fields from a large table with tons of fields.

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


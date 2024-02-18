# Split

> Split transform plugin

## Description

Split a field to more than one field.

## Options

|     name      |  type  | required | default value |
|---------------|--------|----------|---------------|
| separator     | string | yes      |               |
| split_field   | string | yes      |               |
| output_fields | array  | yes      |               |

### separator [string]

The list of fields that need to be kept. Fields not in the list will be deleted

### split_field[string]

The field to be split

### output_fields[array]

The result fields after split

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

We want split `name` field to `first_name` and `second name`, we can add `Split` transform like this

```
transform {
  Split {
    source_table_name = "fake"
    result_table_name = "fake1"
    separator = " "
    split_field = "name"
    output_fields = [first_name, second_name]
  }
}
```

Then the data in result table `fake1` will like this

|   name   | age | card | first_name | last_name |
|----------|-----|------|------------|-----------|
| Joy Ding | 20  | 123  | Joy        | Ding      |
| May Ding | 20  | 123  | May        | Ding      |
| Kin Dom  | 20  | 123  | Kin        | Dom       |
| Joy Dom  | 20  | 123  | Joy        | Dom       |

## Changelog

### new version

- Add Split Transform Connector


# FieldMapper

> FieldMapper transform plugin

## Description

Add input schema and output schema mapping.

## Options

|     name     |  type  | required | default value |
|--------------|--------|----------|---------------|
| field_mapper | Object | yes      |               |

### field_mapper [config]

Specify the field mapping relationship between input and output

### common options [config]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details.

## Example

The data read from source is a table like this:

| id |   name   | age | card |
|----|----------|-----|------|
| 1  | Joy Ding | 20  | 123  |
| 2  | May Ding | 20  | 123  |
| 3  | Kin Dom  | 20  | 123  |
| 4  | Joy Dom  | 20  | 123  |

We want to delete `age` field and update the filed order to `id`, `card`, `name` and rename `name` to `new_name`. We can add `FieldMapper` transform like this

```
transform {
  FieldMapper {
    source_table_name = "fake"
    result_table_name = "fake1"
    field_mapper = {
        id = id
        card = card
        name = new_name
    }
  }
}
```

Then the data in result table `fake1` will like this

| id | card | new_name |
|----|------|----------|
| 1  | 123  | Joy Ding |
| 2  | 123  | May Ding |
| 3  | 123  | Kin Dom  |
| 4  | 123  | Joy Dom  |

## Changelog

### new version

- Add Copy Transform Connector


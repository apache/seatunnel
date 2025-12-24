# Copy

> Copy transform plugin

## Description

Copy a field to a new field.

## Options

|  name  |  type  | required | default value |
|--------|--------|----------|---------------|
| fields | Object | yes      |               |

### fields [config]

Specify the field copy relationship between input and output

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

We want copy fields `name`、`age` to a new fields `name1`、`name2`、`age1`, we can add `Copy` Transform like this

```
transform {
  Copy {
    plugin_input = "fake"
    plugin_output = "fake1"
    fields {
      name1 = name
      name2 = name
      age1 = age
    }
  }
}
```

Then the data in result table `fake1` will like this

|   name   | age | card |  name1   |  name2   | age1 |
|----------|-----|------|----------|----------|------|
| Joy Ding | 20  | 123  | Joy Ding | Joy Ding | 20   |
| May Ding | 20  | 123  | May Ding | May Ding | 20   |
| Kin Dom  | 20  | 123  | Kin Dom  | Kin Dom  | 20   |
| Joy Dom  | 20  | 123  | Joy Dom  | Joy Dom  | 20   |

## Changelog

### new version

- Add Copy Transform Connector
- Support copy fields to a new fields


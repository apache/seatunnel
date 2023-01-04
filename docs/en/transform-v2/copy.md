# Copy

> Copy transform plugin

## Description

Copy a field to a new field.

## Options

| name          | type   | required | default value |
|---------------| ------ | -------- |---------------|
| src_field     | string | yes      |               |
| dest_field    | string | yes      |               |

### src_field [string]

Src field name you want to copy

### dest_field [string]

This dest field name

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

The data read from source is a table like this:

| name     | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

We want copy field `name` to a new field `name1`, we can add `Copy` Transform like this

```
transform {
  Copy {
    source_table_name = "fake"
    result_table_name = "fake1"
    src_field = "name"
    dest_field = "name1"
  }
}
```

Then the data in result table `fake1` will like this

| name     | age | card | name1    |
|----------|-----|------|----------|
| Joy Ding | 20  | 123  | Joy Ding |
| May Ding | 20  | 123  | May Ding |
| Kin Dom  | 20  | 123  | Kin Dom  |
| Joy Dom  | 20  | 123  | Joy Dom  |


## Changelog

### new version

- Add Copy Transform Connector
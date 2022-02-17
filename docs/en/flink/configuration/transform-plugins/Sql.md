# Sql

> Transform plugin : SQL [Flink]

## Description

Use SQL to process data, use `flink sql` grammar, and support its various `udf`

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| sql            | string | yes      | -             |
| common-options | string | no       | -             |

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](./transform-plugin.md) for details

## Examples

```bash
sql {
    sql = "select name, age from fake"
}
```

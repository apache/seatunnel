# Kudu

> Source plugin: Kudu [Spark]

## Description

Read data from Kudu.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| kudu_master            | string | yes      | -             |
| kudu_table       | string | yes      | -         |

### kudu_master [string]

Kudu Master

### kudu_table [string]

Kudu Table

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Example

```bash
kudu {
    kudu_master = "master:7051"
    kudu_table = "impala::test_db.test_table"
    result_table_name = "kudu_result_table"
}
```

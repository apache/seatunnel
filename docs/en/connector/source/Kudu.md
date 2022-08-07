# Kudu

> Kudu source connector

## Description

Read data from Kudu.

:::tip

Engine Supported and plugin name

* [x] Spark: Kudu
* [ ] Flink

:::

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

Source Plugin common parameters, refer to [Source Plugin](common-options.mdx) for details

## Example

```bash
kudu {
    kudu_master = "master:7051"
    kudu_table = "impala::test_db.test_table"
    result_table_name = "kudu_result_table"
}
```

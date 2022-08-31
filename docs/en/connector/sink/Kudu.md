# Kudu

> Kudu sink connector

## Description

Write data to Kudu.

:::tip

Engine Supported and plugin name

* [x] Spark: Kudu
* [ ] Flink

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [kudu_master](#kudu_master-string)            | string | yes      | -             |
| [kudu_table](#kudu_table-string)       | string | yes      | -         |
| [mode](#mode-string)       | string | no      | insert         |

### kudu_master [string]
Kudu master, multiple masters are separated by commas

### kudu_table [string]
The name of the table to be written in kudu, the table must already exist

### mode [string]
Write the mode adopted in kudu, support insert|update|upsert|insertIgnore, the default is insert.
## Example

```bash
kudu {
   kudu_master="hadoop01:7051,hadoop02:7051,hadoop03:7051"
   kudu_table="my_kudu_table"
   mode="upsert"
 }
```

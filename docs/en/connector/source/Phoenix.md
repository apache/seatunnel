# Phoenix

> Phoenix source connector

## Description

Read external data source data through `Phoenix` , compatible with `Kerberos`  authentication

:::tip

Engine Supported and plugin name

* [x] Spark: Phoenix
* [ ] Flink

:::

## Options

| name       | type   | required | default value |
| ---------- | ------ | -------- | ------------- |
| zk-connect | string | yes      | -             |
| table      | string | yes      |               |
| columns    | string | no       | -             |
| tenantId   | string | no       | -             |
| predicate  | string | no       | -             |

### zk-connect [string]

Connection string, configuration example: `host1:2181,host2:2181,host3:2181 [/znode]`

### table [string]

Source data table name

### columns [string-list]

Read column name configuration. Read all columns set to `[]` , optional configuration item, default is `[]` .

### tenant-id [string]

Tenant ID, optional configuration item

### predicate [string]

Conditional filter string configuration, optional configuration items

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details

## Example

```bash
Phoenix {
  zk-connect = "host1:2181,host2:2181,host3:2181"
  table = "table22"
  result_table_name = "tmp1"
}
```

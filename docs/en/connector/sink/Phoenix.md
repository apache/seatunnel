# Phoenix

> Phoenix sink connector

## Description

Export data to `Phoenix` , compatible with `Kerberos` authentication

:::tip

Engine Supported and plugin name

* [x] Spark: Phoenix
* [ ] Flink

:::

## Options

| name                      | type    | required | default value |
| ------------------------- | ------- | -------- | ------------- |
| zk-connect                | array   | yes      | -             |
| table                     | string  | yes      | -             |
| tenantId                  | string  | no       | -             |
| skipNormalizingIdentifier | boolean | no       | false         |
| common-options            | string  | no       | -             |

### zk-connect [string]

Connection string, configuration example: `host1:2181,host2:2181,host3:2181 [/znode]`

### table [string]

Target table name

##### tenantId [string]

Tenant ID, optional configuration item

### skipNormalizingIdentifier [boolean]

Whether to skip the normalized identifier, if the column name is surrounded by double quotes, it is used as is, otherwise the name is uppercase. Optional configuration items, the default is `false`

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](common-options.md) for details

## Examples

```bash
  Phoenix {
    zk-connect = "host1:2181,host2:2181,host3:2181"
    table = "tableName"
  }
```

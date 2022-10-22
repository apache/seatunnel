# Kudu

> Kudu source connector

## Description

Used to read data from Kudu.

 The tested kudu version is 1.11.1.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                     | type    | required | default value |
|--------------------------|---------|----------|---------------|
| kudu_master              | string  | yes      | -             |
| kudu_table               | string  | yes      | -             |
| columnsList              | string  | yes      | -             |
| common-options           |         | no       | -             |

### kudu_master [string]

`kudu_master` The address of kudu master,such as '192.168.88.110:7051'.

### kudu_table [string]

`kudu_table` The name of kudu table..

### columnsList [string]

`columnsList` Specifies the column names of the table.

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Examples

```hocon
source {
   KuduSource {
      result_table_name = "studentlyh2"
      kudu_master = "192.168.88.110:7051"
      kudu_table = "studentlyh2"
      columnsList = "id,name,age,sex"
    }

}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Kudu Source Connector

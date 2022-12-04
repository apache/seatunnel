# Maxcompute

> Maxcompute source connector

## Description

Used to read data from Maxcompute.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                    | type   | required  | default value |
|-------------------------|--------|-----------|---------------|
| accessId                | string | yes       | -             |
| accesskey               | string | yes       | -             |
| endpoint                | string | yes       | -             |
| project                 | string | yes       | -             |
| result_table_name       | string | yes       | -             |
| partition_spec          | string | no        | -             |
| split_row               | int    | no        | 10000         |

### accessId [string]

`accessId` Your Maxcompute accessId which cloud be access from Alibaba Cloud.

### accesskey [string]

`accesskey` Your Maxcompute accessKey which cloud be access from Alibaba Cloud.

### endpoint [string]

`endpoint` Your Maxcompute endpoint start with http.

### project [string]

`project` Your Maxcompute project which is created in Alibaba Cloud.

### result_table_name [string]

`result_table_name` Target Maxcompute table name eg: fake.

### partition_spec [string]

`partition_spec` This spec of Maxcompute partition table eg:ds='20220101'.

### split_row [int]

`split_row` Number of rows per split, default: 10000.

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Examples

```hocon
source {
  MaxcomputeSource {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>"
    result_table_name="<your table name>"
    #partition_spec="<your partition spec>"
    #split_row = 10000
  }
}
```

## Changelog

### 2.1.3-beta 2022-12-05

- Add Maxcompute Source Connector

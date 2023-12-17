# Maxcompute

> Maxcompute sink connector

## Description

Used to read data from Maxcompute.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|      name      |  type   | required | default value |
|----------------|---------|----------|---------------|
| accessId       | string  | yes      | -             |
| accesskey      | string  | yes      | -             |
| endpoint       | string  | yes      | -             |
| project        | string  | yes      | -             |
| table_name     | string  | yes      | -             |
| partition_spec | string  | no       | -             |
| overwrite      | boolean | no       | false         |
| common-options | string  | no       |               |

### accessId [string]

`accessId` Your Maxcompute accessId which cloud be access from Alibaba Cloud.

### accesskey [string]

`accesskey` Your Maxcompute accessKey which cloud be access from Alibaba Cloud.

### endpoint [string]

`endpoint` Your Maxcompute endpoint start with http.

### project [string]

`project` Your Maxcompute project which is created in Alibaba Cloud.

### table_name [string]

`table_name` Target Maxcompute table name eg: fake.

### partition_spec [string]

`partition_spec` This spec of Maxcompute partition table eg:ds='20220101'.

### overwrite [boolean]

`overwrite` Whether to overwrite the table or partition, default: false.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Examples

```hocon
sink {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>"
    table_name="<your table name>"
    #partition_spec="<your partition spec>"
    #overwrite = false
  }
}
```

## Changelog

### next version

- [Feature] Add Maxcompute Sink Connector([3640](https://github.com/apache/seatunnel/pull/3640))


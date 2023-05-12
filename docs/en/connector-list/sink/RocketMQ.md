# RocketMQ

> RocketMQ sink connector
>
  ## Description

Write Rows to a Apache RocketMQ topic.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we will use 2pc to guarantee the message is sent to RocketMQ exactly once.

## Options

|         name         |  type   | required |      default value       |
|----------------------|---------|----------|--------------------------|
| topic                | string  | yes      | -                        |
| name.srv.addr        | string  | yes      | -                        |
| acl.enabled          | Boolean | no       | false                    |
| access.key           | String  | no       |                          |
| secret.key           | String  | no       |                          |
| producer.group       | String  | no       | SeaTunnel-producer-Group |
| semantic             | string  | no       | NON                      |
| partition.key.fields | array   | no       | -                        |
| format               | String  | no       | json                     |
| field.delimiter      | String  | no       | ,                        |
| common-options       | config  | no       | -                        |

### topic [string]

`RocketMQ topic` name.

### name.srv.addr [string]

`RocketMQ` name server cluster address.

### semantic [string]

Semantics that can be chosen EXACTLY_ONCE/AT_LEAST_ONCE/NON, default NON.

### partition.key.fields [array]

Configure which fields are used as the key of the RocketMQ message.

For example, if you want to use value of fields from upstream data as key, you can assign field names to this property.

Upstream data is the following:

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

If name is set as the key, then the hash value of the name column will determine which partition the message is sent to.

### format

Data format. The default format is json. Optional text format. The default field separator is ",".
If you customize the delimiter, add the "field_delimiter" option.

### field_delimiter

Customize the field delimiter for data format.

### common options [config]

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Examples

```hocon
sink {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topic = "test-topic-003"
    partition.key.fields = ["name"]
  }
}
```


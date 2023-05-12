# RocketMQ

> RocketMQ source connector

## Description

Source connector for Apache RocketMQ.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|                name                 |  type   | required |       default value        |
|-------------------------------------|---------|----------|----------------------------|
| topics                              | String  | yes      | -                          |
| name.srv.addr                       | String  | yes      | -                          |
| acl.enabled                         | Boolean | no       | false                      |
| access.key                          | String  | no       |                            |
| secret.key                          | String  | no       |                            |
| batch.size                          | int     | no       | 100                        |
| consumer.group                      | String  | no       | SeaTunnel-Consumer-Group   |
| commit.on.checkpoint                | Boolean | no       | true                       |
| schema                              |         | no       | -                          |
| format                              | String  | no       | json                       |
| field.delimiter                     | String  | no       | ,                          |
| start.mode                          | String  | no       | CONSUME_FROM_GROUP_OFFSETS |
| start.mode.offsets                  |         | no       |                            |
| start.mode.timestamp                | Long    | no       |                            |
| partition.discovery.interval.millis | long    | no       | -1                         |
| common-options                      | config  | no       | -                          |

### topics [string]

`RocketMQ topic` name. If there are multiple `topics`, use `,` to split, for example: `"tpc1,tpc2"`.

### name.srv.addr [string]

`RocketMQ` name server cluster address.

### consumer.group [string]

`RocketMQ consumer group id`, used to distinguish different consumer groups.

### acl.enabled [boolean]

If true, access control is enabled, and access key and secret key need to be configured.

### access.key [string]

When ACL_ENABLED is true, access key cannot be empty.

### secret.key [string]

When ACL_ENABLED is true, secret key cannot be empty.

### batch.size [int]

`RocketMQ` consumer pull batch size

### commit.on.checkpoint [boolean]

If true the consumer's offset will be periodically committed in the background.

## partition.discovery.interval.millis [long]

The interval for dynamically discovering topics and partitions.

### schema

The structure of the data, including field names and field types.

## format

Data format. The default format is json. Optional text format. The default field separator is ", ".
If you customize the delimiter, add the "field.delimiter" option.

## field.delimiter

Customize the field delimiter for data format.

## start.mode

The initial consumption pattern of consumers,there are several types:
[CONSUME_FROM_LAST_OFFSET],[CONSUME_FROM_FIRST_OFFSET],[CONSUME_FROM_GROUP_OFFSETS],[CONSUME_FROM_TIMESTAMP]
,[CONSUME_FROM_SPECIFIC_OFFSETS]

## start.mode.timestamp

The time required for consumption mode to be "CONSUME_FROM_TIMESTAMP".

## start.mode.offsets

The offset required for consumption mode to be "CONSUME_FROM_SPECIFIC_OFFSETS".

for example:

```hocon
start.mode.offsets = {
         topic1-0 = 70
         topic1-1 = 10
         topic1-2 = 10
      }
```

### common-options [config]

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Example

### Simple

```hocon
source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test-topic-002"
    consumer.group = "consumer-group"
    parallelism = 2
    batch.size = 20
    schema = {
       fields {
            age = int
            name = string
       }
     }
    start.mode = "CONSUME_FROM_SPECIFIC_OFFSETS"
    start.mode.offsets = {
                test-topic-002-0 = 20
             }
            
  }
}
```


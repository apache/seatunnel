# Source plugin: Redis [Spark]

## Description

Read data from Redis.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| host           | string | yes      | -             |
| port           | int    | no       | 6379          |
| key_pattern    | string | yes      | -             |
| partition      | int    | no       | 3             |
| db_num         | int    | no       | 0             |
| auth           | string | no       | -             |
| timeout        | int    | no       | 2000          |
| common-options | string | yes      | -             |

### host [string]

Redis server address

### port [int]

Redis service port, default 6379

### key_pattern [string]

Redis Key, support fuzzy matching

### partition [int]

Number of Redis shards. The default is 3

### db_num [int]

Redis database index ID. It is connected to db0 by default

### auth [string]

Redis authentication password

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Example

```bash
Redis {
    host = "192.168.1.100"
    port = 6379
    key_pattern = "*keys*"
    partition = 20
    db_num = 2
    result_table_name = "reids_result_table"
}
```

> The returned table is a data table in which both fields are strings

| raw_key   | raw_message |
| --------- | ----------- |
| keys      | xxx         |
| my_keys   | xxx         |
| keys_mine | xxx         |

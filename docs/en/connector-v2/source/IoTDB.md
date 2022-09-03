# IoTDB

> IoTDB source connector

## Description

Read external data source data through IoTDB.

## Key features

- [x] [batch](key-features.md)
- [ ] [stream](key-features.md)
- [x] [exactly-once](key-features.md)
- [x] [schema projection](key-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](key-features.md)

## Options

| name                       | type    | required | default value |
|----------------------------|---------|----------|---------------|
| host                       | string  | yes      | -             |
| port                       | Int     | yes      | -             |
| node_urls                  | string  | yes      | -             |
| sql                        | string  | yes      |          |
| fields                     | config  | yes      | -             |
| fetch_size                 | int     | no       | -             |
| username                   | string  | no       | -             |
| password   | string  | no       | -             |
| lower_bound                | long    | no       | -             |
| upper_bound                | long    | no       | -             |
| num_partitions             | int     | no       | -             |
| thrift_default_buffer_size | int     | no       | -             |
| enable_cache_leader        | boolean | no       | -             |
| version                    | string  | no       | -             |

### single node, you need to set host and port to connect to the remote data source.

**host** [string] the host of the IoTDB when you select host of the IoTDB

**port** [int] the port of the IoTDB when you select

### multi node, you need to set node_urls to connect to the remote data source.

**node_urls** [string] the node_urls of the IoTDB when you select

e.g.

``` 127.0.0.1:8080,127.0.0.2:8080
```

### other parameters

**sql** [string]
execute sql statement e.g.

```
select name,age from test
```

### fields [string]

the fields of the IoTDB when you select

the field type is SeaTunnel field type `org.apache.seatunnel.api.table.type.SqlType`

e.g.

```
fields{
    name=STRING
    age=INT
    }
```

### option parameters

### fetch_size [int]

the fetch_size of the IoTDB when you select

### username [string]

the username of the IoTDB when you select

### password [string]

the password of the IoTDB when you select

### lower_bound [long]

the lower_bound of the IoTDB when you select

### upper_bound [long]

the upper_bound of the IoTDB when you select

### num_partitions [int]

the num_partitions of the IoTDB when you select

### thrift_default_buffer_size [int]

the thrift_default_buffer_size of the IoTDB when you select

### enable_cache_leader [boolean]

enable_cache_leader of the IoTDB when you select

### version [string]

Version represents the SQL semantic version used by the client, which is used to be compatible with the SQL semantics of
0.12 when upgrading 0.13. The possible values are: V_0_12, V_0_13.

### split partitions

we can split the partitions of the IoTDB and we used time column split

#### num_partitions [int]

split num

### upper_bound [long]

upper bound of the time column

### lower_bound [long]

lower bound of the time column

```
     split the time range into numPartitions parts
     if numPartitions is 1, use the whole time range
     if numPartitions < (upper_bound - lower_bound), use (upper_bound - lower_bound) partitions
     
     eg: lower_bound = 1, upper_bound = 10, numPartitions = 2
     sql = "select * from test where age > 0 and age < 10"
     
     split result

     split 1: select * from test  where (time >= 1 and time < 6)  and (  age > 0 and age < 10 )
     
     split 2: select * from test  where (time >= 6 and time < 11) and (  age > 0 and age < 10 )

```


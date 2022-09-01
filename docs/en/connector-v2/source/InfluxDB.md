# InfluxDB

> InfluxDB source connector

## Description

Read external data source data through InfluxDB. Currently supports Batch mode.

## Options

| name        | type    | required | default value |
|-------------|---------|----------|------------|
| url         | string  | yes      | -          |
| sql         | string  | yes      | -          |
| database            | string  | yes      |            |
| username    | string  | no       | -          |
| password    | string  | no       | -          |
| lower_bound | long    | no       | -          |
| upper_bound | long    | no       | -          |
| partition_num | int     | no       | -          |
| split_column | string     | no       | -          |
| epoch | string     | no       | n           |

### url
the url to connect to influxDB e.g.
``` 
http://influxdb-host:8086
```

### sql [string]
The query sql used to search data

```
select name,age from test
```

### database [string]

The `influxDB` database

### username [string]

the username of the influxDB when you select

### password [string]

the password of the influxDB when you select

### split_column [string]

the `split_column` of the influxDB when you select

> Tips:
> - influxDB tags is not supported as a segmented primary key because the type of tags can only be a string
> - influxDB time is not supported as a segmented primary key because the time field cannot participate in mathematical calculation
> - Currently, `split_column` only supports integer data segmentation, and does not support `float`, `string`, `date` and other types.

### upper_bound [long]

upper bound of the `split_column`column

### lower_bound [long]

lower bound of the `split_column` column

```
     split the $split_column range into $partition_num parts
     if partition_num is 1, use the whole `split_column` range
     if partition_num < (upper_bound - lower_bound), use (upper_bound - lower_bound) partitions
     
     eg: lower_bound = 1, upper_bound = 10, partition_num = 2
     sql = "select * from test where age > 0 and age < 10"
     
     split result

     split 1: select * from test where ($split_column >= 1 and $split_column < 6)  and (  age > 0 and age < 10 )
     
     split 2: select * from test where ($split_column >= 6 and $split_column < 11) and (  age > 0 and age < 10 )

```

### partition_num [string]

the `partition_num` of the InfluxDB when you select
> Tips: Ensure that `upper_bound` minus `lower_bound` is divided `bypartition_num`, otherwise the query results will overlap

### epoch [int]
returned time precision
- Optional values: H, m, s, MS, u, n
- default value: n
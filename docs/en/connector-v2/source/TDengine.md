# TDengine

> TDengine source connector

## Description

Read external data source data through TDengine.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                       | type    | required | default value |
|----------------------------|---------|----------|---------------|
| url                       | string  | yes      | -             |
| username                       | string     | yes      | -             |
| password                  | string  | yes      | -             |
| database                        | string  | yes      |          |
| stable                     | string  | yes      | -             |
| partitions_num                 | int     | no       | -             |
| fields                   | config  | no       | -             |
| lower_bound                | long    | no       | -             |
| upper_bound                | long    | no       | -             |

### url [string] 

the url of the TDengine when you select the TDengine

e.g.
```
jdbc:TAOS-RS://localhost:6041/
```

### fields [string]

the fields of the TDengine when you select

the field type is SeaTunnel field type `org.apache.seatunnel.api.table.type.SqlType`

e.g.

```
      fields {
        ts = "timestamp"
        current = "float"
        voltage = "int"
        phase = "float"
        location = "string"
        groupid = "int"
      }
```

### username [string]

the username of the TDengine when you select

### password [string]

the password of the TDengine when you select

### database [string]

the database of the TDengine when you select

### stable [string]

the stable of the TDengine when you select

### lower_bound [long]

the lower_bound of the migration period

### upper_bound [long]

the upper_bound of the migration period

### partitions_num [int]

the partitions_num of the migration data


```
     split the time range into partitions_num parts
     if numPartitions is 1, use the whole time range
     if numPartitions < (upper_bound - lower_bound), use (upper_bound - lower_bound) partitions
     
     eg: lower_bound = 1, upper_bound = 10, partitions_num = 2
     sql = "select * from test"
     
     split result

     split 1: select * from test  where (ts >= 1 and ts < 6) 
     
     split 2: select * from test  where (ts >= 6 and ts < 11)

```


# TDengine

> TDengine source connector

## Description

Read external data source data through TDengine.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                       | type    | required | default value |
|----------------------------|---------|----------|---------------|
| url                       | string  | yes      | -             |
| username                       | string     | yes      | -             |
| password                  | string  | yes      | -             |
| database                        | string  | yes      |          |
| stable                     | string  | yes      | -             |
| fields                   | config  | no       | -             |
| tags                   | config  | no       | -             |
| lower_bound                | long    | no       | -             |
| upper_bound                | long    | no       | -             |

### url [string] 

the url of the TDengine when you select the TDengine

e.g.
```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

the username of the TDengine when you select

### password [string]

the password of the TDengine when you select

### database [string]

the database of the TDengine when you select

### stable [string]

the stable of the TDengine when you select


### fields [config]

the fields of the TDengine stable

e.g.

```hocon
      fields {
        ts = "timestamp"
        current = "float"
        voltage = "int"
        phase = "float"
        location = "string"
        groupid = "int"
      }
```

### tags [config]

the tags of the TDengine stable

e.g.

```hocon
        tags {
          location = "string"
          groupid = "int"
        }
```


### lower_bound [long]

the lower_bound of the migration period

### upper_bound [long]

the upper_bound of the migration period

## Example

### source
```hocon
source {
    TDengine {
      url : "jdbc:TAOS-RS://localhost:6041/"
      username : "root"
      password : "taosdata"
      database : "power"
      stable : "meters"
      lower_bound : 1538548685000
      upper_bound : 1538548696800
      partitions_num : 2
      fields {
        ts = "timestamp"
        current = "float"
        voltage = "int"
        phase = "float"
      }
      tags {
        location = "string"
        groupid = "int"
      }
      result_table_name = "tdengine_result"
    }
}
```
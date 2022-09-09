# TDengine

> TDengine sink connector

## Description

Used to write data to TDengine.

## Key features

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

### fields [string]

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
### tags [string]

the tags of the TDengine stable

e.g.

```hocon
        tags {
          location = "string"
          groupid = "int"
        }
```

## Example

### sink

```hocon
sink {
      TDengine {
        url : "jdbc:TAOS-RS://localhost:6041/"
        username : "root"
        password : "taosdata"
        database : "power2"
        stable : "meters2"
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
      }
}
```
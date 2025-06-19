import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine sink connector

## Description

Used to write data to TDengine. You need to create stable before running seatunnel task

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Options

|   name   |  type  | required | default value |
|----------|--------|----------|---------------|
| url      | string | yes      | -             |
| username | string | yes      | -             |
| password | string | yes      | -             |
| database | string | yes      |               |
| stable   | string | yes      | -             |
| timezone | string | no       | UTC           |

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

### timezone [string]

the timeznoe of the TDengine sever, it's important to the ts field

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
          timezone: UTC
        }
}
```

## Changelog

<ChangeLog />
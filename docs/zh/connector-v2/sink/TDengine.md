# TDengine

> TDengine 水槽连接器

## 描述

Used to write data to TDengine. You need to create stable before running seatunnel task

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## 选项

|   名称   |  类型  | 需要 | 默认 值 |
|----------|--------|----------|---------------|
| url      | string | 是      | -             |
| username | string | 是      | -             |
| password | string | 是      | -             |
| database | string | 是      |               |
| stable   | string | 是      | -             |
| timezone | string | 否       | UTC           |

### url [string]

选择TDengine时的TDengine的url

例如

```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

选择时TDengine的用户名

### password [string]

选择时TD引擎的密码

### database [string]

当您选择时，TDengine的数据库

### stable [string]

选择时TD引擎的稳定性

### timezone [string]

TDengine服务器的时间对ts领域很重要

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


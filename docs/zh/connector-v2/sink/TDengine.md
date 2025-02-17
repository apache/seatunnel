# TDengine

> TDengine 数据接收器

## 描述

用于将数据写入TDengine. 在运行 seatunnel 任务之前，你需要创建稳定的环境。

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## 选项

|   名称   |  类型  | 是否必传 | 默认值 |
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

选择时TDengine的密码

### database [string]

当您选择时，TDengine的数据库

### stable [string]

选择时TDengine的稳定性

### timezone [string]

TDengine服务器的时间对ts领域很重要

## 示例

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


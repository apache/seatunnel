import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine 数据接收器

## 描述

用于将数据写入TDengine。

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

TDengine的url

例如

```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

TDengine的用户名

### password [string]

TDengine的密码

### database [string]

TDengine的数据库

### stable [string]

TDengine的超级表

### timezone [string]

TDengine服务器的时间，对ts字段很重要

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

## 变更日志

<ChangeLog />
import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine 源端连接器

## 描述

通过 TDengine 读取外部数据源的数据。

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流式](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)

支持查询 SQL，并可实现投影效果。

- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 配置项

| 名称           | 类型   | 必填 | 默认值         |
|----------------|--------|------|----------------|
| url            | string | 是   | -              |
| username       | string | 是   | -              |
| password       | string | 是   | -              |
| database       | string | 是   |                |
| stable         | string | 是   | -              |
| sub_tables     | list   | 否   | -              |
| lower_bound    | long   | 是   | -              |
| upper_bound    | long   | 是   | -              |
| read_columns   | list   | 否   | -              |

### url [string]

选择 TDengine 时的连接 URL

例如：

```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

选择 TDengine 时的用户名

### password [string]

选择 TDengine 时的密码

### database [string]

选择 TDengine 时的数据库名

### stable [string]

选择 TDengine 时的超级表名

### sub_tables [list]

TDengine 的子表名。如果不指定，则会选择所有子表；如果指定，则只选择指定的子表。

### lower_bound [long]

迁移时间段的下界

### upper_bound [long]

迁移时间段的上界

### read_columns [list]

选择 TDengine 时的列名。如果不指定，则选择所有字段；如果指定，则只选择指定的字段。读取超级表时，请包含TAGS 字段，并放在末尾。

## 示例

### source 配置示例

```hocon
source {
        TDengine {
          url : "jdbc:TAOS-RS://localhost:6041/"
          username : "root"
          password : "taosdata"
          database : "power"
          stable : "meters"
          sub_tables : ["meter_1","meter_2"]
          lower_bound : "2018-10-03 14:38:05.000"
          upper_bound : "2018-10-03 14:38:16.800"
          plugin_output = "tdengine_result"
          read_columns : ["ts","voltage","current","power"]
        }
}
```

## 变更日志

<ChangeLog />
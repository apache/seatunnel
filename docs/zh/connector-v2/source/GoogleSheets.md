# GoogleSheets

> GoogleSheets 源连接器

## 描述

用于从GoogleSheets读取数据.

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)
- [ ] 文件格式
  - [ ] text
  - [ ] csv
  - [ ] json

## 选项

|        名称           |  类型  | 必需 | 默认值 |
|---------------------|--------|----------|---------------|
| service_account_key | string | 是      | -             |
| sheet_id            | string | 是      | -             |
| sheet_name          | string | 是      | -             |
| range               | string | 是      | -             |
| schema              | config | 否       | -             |

### service_account_key [string]

谷歌云服务帐户，需要base64编码

### sheet_id [string]

Google表格URL中的表格id

### sheet_name [string]

要导入的工作表的名称

### range [string]

要导入的 sheet 页的范围

### schema [config]

#### fields [config]

上游数据的字段

## 示例

简单示例:

```hocon
GoogleSheets {
  service_account_key = "seatunnel-test"
  sheet_id = "1VI0DvyZK-NIdssSdsDSsSSSC-_-rYMi7ppJiI_jhE"
  sheet_name = "sheets01"
  range = "A1:C3"
  schema = {
    fields {
      a = int
      b = string
      c = string
    }
  }
}
```

## 变更日志

### 下一个版本

- 添加 GoogleSheets 源连接器


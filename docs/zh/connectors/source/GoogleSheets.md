import ChangeLog from '../changelog/connector-google-sheets.md';

# GoogleSheets

> GoogleSheets 源连接器

## 描述

用于通过 Google Sheets API 从 Google 表格中读取数据。连接器使用 Google Cloud 服务账号凭据读取指定
范围内的内容，并根据用户定义的 schema 把每一行转换为 SeaTunnel 记录。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [ ] 文件格式
  - [ ] text
  - [ ] csv
  - [ ] json

## 数据类型映射

Google Sheets API 不会返回单元格本身的数据类型 —— 每个单元格读取的都是一个无类型的原始值。连接器
会按照用户声明的 `schema` 选项对单元格进行类型转换，因此最终输出的 SeaTunnel 类型完全由 schema 决定，
与表格中单元格的原生类型无关。单元格值无法转换为目标 schema 类型时，对应行会失败。

| Google Sheets 单元格 | SeaTunnel 数据类型（按 schema 转换后） |
|----------------------|---------------------------------------|
| string               | string / 数值 / boolean / 日期         |
| number               | int / long / float / double           |
| boolean              | boolean                               |
| date                 | date / time / timestamp               |

## 源选项

|        名称            |  类型  | 必需 | 默认值 | 描述                                                                                          |
|------------------------|--------|------|--------|-----------------------------------------------------------------------------------------------|
| service_account_key    | string | 是   | -      | Google Cloud 服务账号凭据，必须使用 Base64 编码后的 JSON 字符串。                             |
| sheet_id               | string | 是   | -      | Google 表格的 id，即表格 URL 中 `/d/` 与 `/edit` 之间的长字符串。                             |
| sheet_name             | string | 是   | -      | 要读取的工作表（标签页）名称，例如 `Sheet1`。                                                  |
| range                  | string | 是   | -      | 要读取的 A1 表示法范围，例如 `A1:C3` 或 `Sheet1!A1:D100`。                                    |
| schema                 | config | 否   | -      | 上游数据的字段定义，详见 [Schema 特性](../../introduction/concepts/schema-feature.md)。       |

### service_account_key [string]

Google Cloud 服务账号密钥 JSON 文件经过 Base64 编码后的内容。服务账号必须有目标 Google 表格的访问
权限（请将服务账号邮箱加入表格的共享列表）。

### sheet_id [string]

要读取的 Google 表格 id，即表格 URL 中 `/d/` 与 `/edit` 之间的长字符串。

### sheet_name [string]

要读取的工作表（标签页）名称，例如 `Sheet1`。

### range [string]

要读取的 A1 表示法范围，例如 `A1:C3` 用于读取固定区域，或 `Sheet1!A:D` 用于读取整列。

### schema [config]

#### fields [config]

上游数据的字段定义。连接器会把每个单元格作为字符串读取，然后按声明的类型进行转换。可用的类型请参考
[Schema 特性](../../introduction/concepts/schema-feature.md)。

## 任务示例

### 简单示例

```hocon
source {
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
}
```

### 配合下游接收器

读取一个工作表并通过 Console 接收器打印读取的行数据。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleSheets {
    service_account_key = "seatunnel-test"
    sheet_id = "1VI0DvyZK-NIdssSdsDSsSSSC-_-rYMi7ppJiI_jhE"
    sheet_name = "sheets01"
    range = "A1:C100"
    schema = {
      fields {
        a = int
        b = string
        c = string
      }
    }
  }
}

sink {
  Console {
  }
}
```

## 变更日志

<ChangeLog />

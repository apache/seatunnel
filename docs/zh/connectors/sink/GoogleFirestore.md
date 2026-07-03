import ChangeLog from '../changelog/connector-google-firestore.md';

# GoogleFirestore

> Google Firestore Sink 连接器

## 描述

GoogleFirestore Sink 用于将 SeaTunnel 数据写入 Google Cloud Firestore 集合。

每一行 SeaTunnel 数据会转换为一个 Firestore 文档。连接器必须配置目标
Google Cloud 项目和集合。凭证可以通过 `credentials` 传入 Base64 编码后的
服务账号 JSON；如果不配置该参数，则会使用 Google 应用默认凭证。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           | 类型   | 必填 | 默认值 |
|----------------|--------|------|--------|
| project_id     | string | 是   | -      |
| collection     | string | 是   | -      |
| credentials    | string | 否   | -      |
| common-options |        | 否   | -      |

### project_id [string]

Firestore 数据库所在的 Google Cloud 项目 ID。

### collection [string]

要写入的 Firestore 集合名称。

### credentials [string]

Base64 编码后的 Google Cloud 服务账号 JSON。

如果不配置该参数，连接器会使用 Google 应用默认凭证。此时需要确保
`GOOGLE_APPLICATION_CREDENTIALS` 指向服务账号 JSON 文件，或者运行环境已经提供默认凭证。

### 通用选项

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_date = date
        c_timestamp = timestamp
        c_map = "map<string, string>"
        c_array = "array<int>"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["hello", true, 10, 10000000000, 1.23, "123.456", "2023-04-22", "2023-04-22T23:20:58", {"a": "b"}, [1, 2, 3]]
      }
    ]
  }
}

sink {
  GoogleFirestore {
    project_id = "dummy-project"
    collection = "dummy-collection"
    credentials = "base64-service-account-json"
  }
}
```

## 变更日志

<ChangeLog />

# AmazonDynamoDB

> AmazonDynamoDB 源连接器

## 描述

从 Amazon DynamoDB 读取数据.

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 选项

|         名称        |  类型  | 必需 | 默认值 |
|-----------------------|--------|----|---------------|
| url                   | string | 否  | -             |
| region                | string | 否  | -             |
| access_key_id         | string | 否  | -             |
| secret_access_key     | string | 否  | -             |
| table                 | string | 是  | -             |
| schema                | config | 是  | -             |
| common-options        |        | 是  | -             |
| scan_item_limit       |        | 否  | -             |
| parallel_scan_threads |        | 否  | -             |

### url [string]

读取Amazon Dynamodb的URL. 它将覆盖AWS DynamoDB的`endpoint`.

### region [string]

Amazon DynamoDB 的分区. DynamoDB客户端将使用该区域确定服务端点.

### access_key_id [string]

Amazon DynamoDB的访问id. 如果未设置，插件将使用容器凭证提供程序链获取访问id.

### secret_access_key [string]

Amazon DynamoDB的访问密钥. 如果未设置，插件将使用容器凭证提供程序链获取访问密钥.

### table [string]

Amazon DynamoDB 的表名.

### schema [Config]

#### fields [config]

Amazon Dynamodb是一个支持键值存储和文档数据结构的NOSQL数据库服务，无法获取数据类型。因此，我们必须配置模式.

例如:

```
schema {
  fields {
    id = int
    key_aa = string
    key_bb = string
  }
}
```

### common options

源插件常用参数，详见 [Source Plugin](../source-common-options.md) 

### scan_item_limit

每个扫描请求应返回的项目数

### parallel_scan_threads

并行扫描的逻辑段数

## 例子

```bash
Amazondynamodb {
  url = "http://127.0.0.1:8000"
  region = "us-east-1"
  accessKeyId = "dummy-key"
  secretAccessKey = "dummy-secret"
  table = "TableName"
  schema = {
    fields {
      artist = string
      c_map = "map<string, array<int>>"
      c_array = "array<int>"
      c_string = string
      c_boolean = boolean
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
      c_decimal = "decimal(30, 8)"
      c_null = "null"
      c_bytes = bytes
      c_date = date
      c_timestamp = timestamp
    }
  }
}
```

## 变更日志

### 下一个版本

- 添加Amazon DynamoDB源连接器
- 将源代码拆分添加到Amazondynamodb连接器


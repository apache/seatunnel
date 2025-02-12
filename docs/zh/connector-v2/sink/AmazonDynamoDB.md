# AmazonDynamoDB

> Amazon DynamoDB 接收器连接器

## 描述

将数据写入 Amazon DynamoDB

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|       名称        |  类型  | 必需 | 默认值 |
|-------------------|--------|----------|---------------|
| url               | string | yes      | -             |
| region            | string | yes      | -             |
| access_key_id     | string | yes      | -             |
| secret_access_key | string | yes      | -             |
| table             | string | yes      | -             |
| batch_size        | string | no       | 25            |
| common-options    |        | no       | -             |

### url [string]

要写入Amazon DynamoDB的URL.

### region [string]

Amazon DynamoDB 的区域.

### accessKeyId [string]

Amazon DynamoDB的访问id.

### secretAccessKey [string]

Amazon DynamoDB的访问密钥.

### table [string]

Amazon DynamoDB 的表名.

### 常见选项

Sink插件常用参数，请参考 [Sink Common Options](../sink-common-options.md) 了解详细信息.

## 示例

```bash
Amazondynamodb {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    accessKeyId = "dummy-key"
    secretAccessKey = "dummy-secret"
    table = "TableName"
  }
```

## 变更日志

### 下一个版本

- 添加Amazon DynamoDB接收器连接器


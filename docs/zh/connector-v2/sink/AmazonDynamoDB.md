# AmazonDynamoDB

> Amazon DynamoDB 接收器连接器

## 描述

将数据写入 Amazon DynamoDB

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|       名称        |  类型  | 必需 | 默认值 |
|-------------------|--------|----|---------------|
| url               | string | 否  | -             |
| region            | string | 否  | -             |
| access_key_id     | string | 否  | -             |
| secret_access_key | string | 否  | -             |
| table             | string | 是  | -             |
| batch_size        | string | 否  | 25            |
| common-options    |        | 否  | -             |

### url [string]

要写入Amazon DynamoDB的URL. 它将覆盖AWS DynamoDB的`endpoint`.

### region [string]

Amazon DynamoDB 的分区. DynamoDB客户端将使用该区域确定服务端点. 

### access_key_id [string]

Amazon DynamoDB的访问id. 如果未设置，插件将使用容器凭证提供程序链获取访问id.

### secret_access_key [string]

Amazon DynamoDB的访问密钥. 如果未设置，插件将使用容器凭证提供程序链获取访问密钥.

### table [string]

Amazon DynamoDB 的表名.

### batch_size [string]

在批处理中写入Amazon DynamoDB的记录数. 默认值为25，最大值为25，因为Amazon DynamoDB批处理写入项限制为25.

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

- 添加 Amazon DynamoDB 接收器连接器


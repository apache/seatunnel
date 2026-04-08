import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB 接收器连接器

## 描述

将数据写入 Amazon DynamoDB

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 选项

|       名称        |  类型  | 必需 | 默认值 |
|-------------------|--------|----|---------------|
| url               | string | 是  | -             |
| region            | string | 是  | -             |
| access_key_id     | string | 是  | -             |
| secret_access_key | string | 是  | -             |
| table             | string | 是  | -             |
| batch_size          | int    | 否  | 25   |
| max_retries         | int    | 否  | 10   |
| retry_base_delay_ms | long   | 否  | 100  |
| retry_max_delay_ms  | long   | 否  | 5000 |
| common-options      |        | 否  | -    |

### url [string]

要写入Amazon DynamoDB的URL.

### region [string]

Amazon DynamoDB 的分区.

### access_key_id [string]

Amazon DynamoDB的访问id.

### secret_access_key [string]

Amazon DynamoDB的访问密钥.

### table [string]

Amazon DynamoDB 的表名. 支持使用 `${table_name}` 占位符，用于多表写入场景.

### batch_size [int]

写入 Amazon DynamoDB 前批量缓存的记录数.

### max_retries [int]

当 DynamoDB 返回未处理数据时，批量写入请求的最大重试次数.

### retry_base_delay_ms [long]

重试之间指数退避的基础延迟时间（毫秒）.

### retry_max_delay_ms [long]

重试之间的最大延迟时间（毫秒）.

### 常见选项

Sink插件常用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md) 了解详细信息.

## 示例

### 单表写入
```bash
AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "TableName"
}
```

### 多表写入
```bash
AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "${table_name}"
}
```

## 变更日志

<ChangeLog />

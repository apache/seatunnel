import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus数据接收器

## 描述

Milvus sink连接器将数据写入Milvus或Zilliz Cloud，它具有以下功能：
- 支持按分区读写数据
- 支持从元数据列写入动态模式数据
- json数据将转换为json字符串进行写入
- 自动重试以绕过 ratelimit 限制 和 grpc 限制
## 主要特性

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

##数据类型映射

|  Milvus数据类型   | SeaTunnel 数据类型 |
|---------------------|---------------------|
| INT8                | TINYINT             |
| INT16               | SMALLINT            |
| INT32               | INT                 |
| INT64               | BIGINT              |
| FLOAT               | FLOAT               |
| DOUBLE              | DOUBLE              |
| BOOL                | BOOLEAN             |
| JSON                | STRING              |
| ARRAY               | ARRAY               |
| VARCHAR             | STRING              |
| FLOAT_VECTOR        | FLOAT_VECTOR        |
| BINARY_VECTOR       | BINARY_VECTOR       |
| FLOAT16_VECTOR      | FLOAT16_VECTOR      |
| BFLOAT16_VECTOR     | BFLOAT16_VECTOR     |
| SPARSE_FLOAT_VECTOR | SPARSE_FLOAT_VECTOR |

## Sink 选项

|         名字         | 类型    | 是否必传 |           默认值            | 描述                                               |
|----------------------|---------|----------|------------------------------|-----------------------------------------------------------|
| url                  | String  | 是      | -                            | 连接到Milvus或Zilliz Cloud的URL。             |
| token                | String  | 是      | -                            | 用户：密码                                             |
| database             | String  | 否       | -                            | 将数据写入哪个数据库，默认为源数据库。 |
| schema_save_mode     | enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | 当表不存在时自动创建表。                   |
| enable_auto_id       | boolean | 否       | false                        | 主键列启用autoId。                         |
| enable_upsert        | boolean | 否       | false                        | 是否启用upsert。                                   |
| enable_dynamic_field | boolean | 否       | true                         | 是否启用带动态字段的创建表。                   |
| batch_size           | int     | 否       | 1000                         | 写入批大小。                                         |
| partition_key        | String  | 否       |                              | Milvus分区键字段                                |                                         

## 任务示例

```bash
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    batch_size = 1000
  }
}
```

## 变更日志

<ChangeLog />
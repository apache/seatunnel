# Milvus

> Milvus 源连接器

## 描述

这个Milvus源连接器从Milvus或Zilliz Cloud读取数据，它具有以下功能：
- 支持按分区读写数据
- 支持将动态模式数据读入元数据列
- json数据将转换为json字符串，并将sink转换为json
- 自动重试以绕过速率限制和grpc限制

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)

## 数据类型映射

|  Milvus 数据类型   | SeaTunnel 数据类型 |
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

## 源选项

|    名称           |  类型  | 必需 | 默认值 |                                        描述                                         |
|------------|--------|----------|---------|--------------------------------------------------------------------------------------------|
| url        | String | 是      | -       | 连接到Milvus或Zilliz Cloud的URL.                                              |
| token      | String | 是      | -       | 用户：密码                                                                            |
| database   | String | 是      | default | 从哪个数据库读取数据.                                                             |
| collection | String | 否       | -       | 如果设置，将只读取一个集合，否则将读取数据库下的所有集合. |

## 任务示例

```bash
source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
  }
}
```

## 变更日志


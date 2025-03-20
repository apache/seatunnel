import ChangeLog from '../changelog/connector-aerospike.md';

# Aerospike

> Aerospike 数据写入连接器

## 许可证兼容性通知

此连接器依赖于根据AGPL 3.0许可的Aerospike客户端库。
使用此连接器时，您需要遵守AGPL 3.0许可条款。

## 支持引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [CDC](../../concept/connector-v2-features.md)

## 描述

用于向 Aerospike 数据库写入数据的连接器。

## 支持的数据源

|   数据源    | 支持版本 | Maven 依赖                                                              |
|------------|---|-------------------------------------------------------------------------|
| Aerospike  | 4.4.17+ | [下载](https://mvnrepository.com/artifact/com.aerospike/aerospike-client) |

## 数据类型映射

| SeaTunnel 数据类型 | Aerospike 数据类型 | 存储格式                                                                       |
|----------------|--------------------|------------------------------------------------------------------------------|
| STRING         | STRING             | 直接存储字符串                                                               |
| INT            | INTEGER            | 32位整型                                                                     |
| BIGINT         | LONG               | 64位整型                                                                     |
| DOUBLE         | DOUBLE             | 64位浮点数                                                                   |
| BOOLEAN        | BOOLEAN            | 存储为 true/false 值                                                         |
| ARRAY          | BYTEARRAY          | 仅支持字节数组类型                                                           |
| LIST           | LIST               | 支持泛型列表类型                                                             |
| DATE           | LONG               | 转换为纪元时间毫秒数                                                        |
| TIMESTAMP      | LONG               | 转换为纪元时间毫秒数                                                        |

注意事项：
- 使用ARRAY类型时，SeaTunnel数组元素必须是byte类型
- LIST类型支持可序列化的任意元素类型
- DATE/TIMESTAMP转换使用系统默认时区

## 配置选项

| 参数名称        | 类型    | 必填 | 默认值  | 说明                                                                 |
|----------------|---------|------|---------|---------------------------------------------------------------------|
| host           | string  | 是   | -       | Aerospike 服务器主机名或IP地址                                      |
| port           | int     | 否   | 3000    | Aerospike 服务器端口                                                |
| namespace      | string  | 是   | -       | Aerospike 命名空间                                                  |
| set            | string  | 是   | -       | Aerospike 集合名称                                                  |
| username       | string  | 否   | -       | 认证用户名                                                          |
| password       | string  | 否   | -       | 认证密码                                                            |
| key            | string  | 是   | -       | 用作 Aerospike 主键的字段名称                                       |
| bin_name       | string  | 否   | -       | 数据存储的 bin 名称                                                 |
| data_format    | string  | 否   | string  | 数据存储格式：map/string/kv                                         |
| write_timeout  | int     | 否   | 200     | 写入操作超时时间（毫秒）                                            |
| schema.field   | map     | 否   | {}      | 字段类型映射（示例：{"name":"STRING","age":"INTEGER"}）             |

### data_format 选项说明
- **map**: 以JSON对象格式存储
- **string**: 以JSON字符串格式存储
- **kv**: 每个字段存储为独立的bin

## 任务示例

### 简单示例

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        address = "string"
      }
    }
  }
}

sink {
  Aerospike {
    host = "localhost"
    port = 3000
    namespace = "test_namespace"
    set = "user_data"
    key = "id"
    data_format = "map"
    write_timeout = 300
    schema.field = {
      id = "INTEGER"
      name = "STRING"
      age = "INTEGER"
      address = "STRING"
    }
  }
}
```
## Changelog

<ChangeLog />

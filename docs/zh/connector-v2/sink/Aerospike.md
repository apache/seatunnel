# Aerospike

> Aerospike 数据写入连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [CDC](../../concept/connector-v2-features.md)

## 描述

用于向 Aerospike 数据库写入数据的连接器。

## 支持的数据源

|   数据源    | 支持版本 | Maven 依赖                                                              |
|------------|---|-------------------------------------------------------------------------|
| Aerospike  | 4.4.17+ | [下载](https://mvnrepository.com/artifact/com.aerospike/aerospike-client) |

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

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

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

用于向 Aerospike 数据库写入数据的连接器。该连接器会把数据写入一个指定的 Aerospike 命名空间和集合，并使用 `key` 指定的字段作为 Aerospike 记录主键。

该连接器只写入一个固定的目标集合，不会按表名自动把数据路由到不同的 Aerospike 集合。

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
| host           | string  | 是   | -       | Aerospike 服务器主机名或 IP 地址。                                  |
| port           | int     | 是   | 3000    | Aerospike 服务器端口。                                              |
| namespace      | string  | 是   | -       | Aerospike 命名空间。                                                |
| set            | string  | 是   | -       | Aerospike 集合名称。                                                |
| username       | string  | 否   | -       | 认证用户名。未开启认证时可以不配置。                                |
| password       | string  | 否   | -       | 认证密码。未开启认证时可以不配置。                                  |
| key            | string  | 是   | -       | 用作 Aerospike 记录主键的 SeaTunnel 字段名称，该字段必须存在于输入 schema 中。 |
| bin_name       | string  | 否   | -       | `map` 和 `string` 格式使用的 Aerospike bin 名称，这两种格式下必填。  |
| data_format    | string  | 否   | string  | 数据存储格式，支持 `map`、`string`、`kv`。                           |
| write_timeout  | int     | 否   | 200     | 写入操作超时时间，单位毫秒。                                        |
| schema.field   | map     | 否   | {}      | 字段到 Aerospike 类型的映射，例如：`c_id = "INTEGER"`。             |

### data_format 选项说明

- **map**: 将所有非主键字段作为一个 map 存到 `bin_name`
- **string**: 将所有非主键字段作为 JSON 字符串存到 `bin_name`
- **kv**: 每个非主键字段存储为独立的 bin，此时不使用 `bin_name`

当 `data_format` 为 `map` 或 `string` 时，必须配置 `bin_name`，否则写入器无法判断打包后的整行数据应该写入哪个 Aerospike bin。

### schema.field 配置说明

`schema.field` 对 `map` 和 `string` 格式是可选配置。需要明确控制每个字段写入 Aerospike 时的类型时再配置。

当 `data_format` 为 `kv` 时，请在 `schema.field` 中列出需要写成独立 Aerospike bin 的字段。写入器会遍历 `schema.field`，未列出的字段不会在 `kv` 模式下写入。

支持的 Aerospike 类型名称包括 `STRING`、`INTEGER`、`LONG`、`DOUBLE`、`BOOLEAN`、`BYTEARRAY`、`LIST`。

## 任务示例

### 将 FakeSource 数据写入 Aerospike

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 9
    string.fake.mode = "template"
    string.template = ["tyrantlucifer", "hailin", "kris", "fanjia", "zongwen", "gaojun"]
    int.fake.mode = "template"
    int.template = [20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
    double.fake.mode = "template"
    double.template = [44.0, 45.0, 46.0, 47.0]
    timestamp.fake.mode = "template"
    timestamp.template = [
      "2022-01-01 00:00:00",
      "2022-01-01 00:00:01",
      "2022-01-01 00:00:02",
      "2022-01-01 00:00:03"
    ]
    schema = {
      fields {
        c_id = "int"
        c_name = "string"
        c_money = "double"
        c_birth = "timestamp"
      }
    }
  }
}

sink {
  Aerospike {
    host = "aerospike-host"
    port = 3000
    namespace = "test"
    set = "seatunnel"
    key = "c_id"
    bin_name = "data"
    data_format = "string"
    username = ""
    password = ""
    schema {
      field {
        c_id = "INTEGER"
        c_name = "STRING"
        c_money = "DOUBLE"
        c_birth = "LONG"
      }
    }
  }
}
```

## 更新日志

<ChangeLog />

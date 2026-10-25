import ChangeLog from '../changelog/connector-http.md';

# Http

> Http Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

接收 Source 端传入的数据，并使用该数据触发 Webhook。Http Sink 始终发送 `POST` 请求；每条上游
数据会被序列化为 JSON，作为请求体发送。

> 例如，来自上游的数据为 [`age: 12, name: tyrantlucifer`]，则 body 内容如下：`{"age": 12, "name": "tyrantlucifer"}`

**提示：Http Sink 仅支持 `POST json` 类型的 Webhook，source 数据将被视为 Webhook 中的 body 内容。**

## 支持的数据源信息

想使用 Http 连接器，需要安装以下必要的依赖。可以通过运行 `install-plugin.sh` 脚本或者从 Maven
中央仓库下载这些依赖。

| 数据源  | 支持版本 | 依赖                                                                            |
|--------|----------|---------------------------------------------------------------------------------|
| Http   | 通用     | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http)   |

## 接收器选项

|             名称              |  类型   | 是否必须 | 默认值 | 描述                                                                                                                                              |
|-------------------------------|--------|----------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| url                           | String | 是       | -      | Http 请求 URL。静态查询参数可以直接拼接在 URL 中。                                                                                                  |
| headers                       | Map    | 否       | -      | 每个请求都会携带的 HTTP 头，每个条目是一个 `name = "value"` 键值对。                                                                                  |
| params                        | Map    | 否       | -      | 当前 Sink 写入器不读取 `params`。请将查询参数直接写入 `url`；行数据作为 POST 请求体发送。 |
| retry                         | Int    | 否       | -      | 大于等于 1 时，表示包含首次请求在内的最大总尝试次数。仅对 `IOException` 进行重试。 |
| retry_backoff_multiplier_ms   | Int    | 否       | 100    | 失败重试时回退时间（毫秒）的乘数。                                                                                                                  |
| retry_backoff_max_ms          | Int    | 否       | 10000  | 失败重试时回退时间（毫秒）的上限。                                                                                                                  |
| array_mode                    | Boolean| 否       | false  | 为 true 时，多条数据会被合并为 JSON 数组发送；为 false 时（默认）每条数据单独作为 JSON 对象发送。                                                              |
| batch_size                    | Int    | 否       | 1      | 单个 HTTP 请求最多发送的数据条数。仅在 `array_mode = true` 时生效。                                                                                  |
| request_interval_ms           | Int    | 否       | 0      | 两次 HTTP 请求之间的间隔毫秒数，用于避免请求过于频繁。                                                                                                |
| multi_table_sink_replica      | Int    | 否       | -      | 多表写入时使用的 Sink 副本数，详情请参考 [Sink 常用选项](../common-options/sink-common-options.md)。                                                       |
| common-options                |        | 否       | -      | Sink 插件常用参数，详情请参考 [Sink 常用选项](../common-options/sink-common-options.md)。                                                              |

### url

Webhook 接收端的 HTTP URL。可以把静态的查询参数直接写在 URL 中，例如
`http://localhost/test/webhook?source=seatunnel`。

### params

Http Sink 始终使用 `POST` 请求，并将行数据转换为 JSON 请求体。当前写入器不读取
`params`；静态查询参数应直接写入 `url`，例如
`http://localhost/test/webhook?source=seatunnel`。

### 重试行为

只有 `IOException` 失败会触发重试。`retry >= 1` 时，它限制包含首次请求在内的总尝试
次数：`retry = 3` 表示最多尝试三次，即最多重试两次。等待时间使用斐波那契序列：
`min(Fibonacci(尝试序号) * retry_backoff_multiplier_ms, retry_backoff_max_ms)`。
默认乘数为 100 ms，对应后续重试等待 100、100、200、300、500 ms，并受
`retry_backoff_max_ms` 上限限制。

当 HTTP 响应不是 200，或请求过程中发生异常时，Sink 会将写入失败上报给引擎；失败批次不会被当作已成功发送。该连接器不提供精确一次投递保证，接收端应保证幂等。

### array_mode 与 batch_size

当 `array_mode = false`（默认）时，Http Sink 每条数据发送一次请求。设置为 `array_mode = true`
后会按批次发送；`batch_size` 控制每个 JSON 数组请求最多包含多少条数据，`request_interval_ms` 用
于在相邻批次之间增加延迟。

## 示例

### 简单示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema = {
      fields {
        age = "int"
        name = "string"
      }
    }
  }
}

sink {
  Http {
    url = "http://localhost/test/webhook"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
  }
}
```

### 带批处理的示例

```hocon
sink {
  Http {
    url = "http://localhost/test/webhook"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
      Content-Type = "application/json"
    }
    array_mode = true
    batch_size = 50
    request_interval_ms = 500
  }
}
```

### 带重试和 URL 查询参数的示例

```hocon
sink {
  Http {
    url = "http://localhost/test/webhook?source=seatunnel&channel=cdc"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
    retry = 3
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 5000
  }
}
```

### 多表写入

在 URL 中使用 `${database_name}` 和 `${table_name}` 占位符，把不同上游表的数据路由到不同的 Webhook 地址。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"

    table-names = ["seatunnel.role", "seatunnel.user", "galileo.Bucket"]
  }
}

transform {
}

sink {
  Http {
    url = "http://localhost/test/${database_name}_test/${table_name}_test"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
  }
}
```

如果上游是带有 schema 限定的表列表（例如 Oracle），可以使用 `${schema_name}` 代替 `${database_name}`：

```hocon
source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    username = "testUser"
    password = "testPassword"

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  Http {
    url = "http://localhost/test/${schema_name}_test/${table_name}_test"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
  }
}
```

## 变更日志

<ChangeLog />

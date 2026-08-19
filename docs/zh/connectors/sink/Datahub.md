import ChangeLog from '../changelog/connector-datahub.md';

# DataHub

> DataHub Sink 连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

DataHub Sink 用于将 SeaTunnel 数据写入阿里云 DataHub。

该连接器支持单表写入和多表写入。多表写入时，可以在 `topic` 中使用
`${table}` 这类占位符，将不同输入表的数据写入不同的 DataHub Topic。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 使用前准备

运行 SeaTunnel 任务前，请先创建 DataHub 项目和 Topic。DataHub Topic 的结构中需要包含和上游 SeaTunnel schema 同名的字段，因为该 Sink 会按字段名写入数据。

## Sink 选项

| 名称             | 类型     | 必填 | 默认值 | 描述                                                              |
|----------------|--------|----|-----|-----------------------------------------------------------------|
| endpoint       | string | 是  | -   | DataHub 服务地址。                                                   |
| accessId       | string | 是  | -   | 访问 DataHub 使用的阿里云 Access ID。                                  |
| accessKey      | string | 是  | -   | 访问 DataHub 使用的阿里云 Access Key。                                 |
| project        | string | 是  | -   | DataHub 项目名称。                                                  |
| topic          | string | 是  | -   | DataHub Topic 名称，多表写入时支持占位符。                                  |
| timeout        | int    | 否  | 3000 | 客户端连接最大超时时间，单位为毫秒。                                            |
| retryTimes     | int    | 否  | 3   | 写入记录失败时的最大重试次数。                                                |
| common-options | config | 否  | -   | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。 |

### endpoint [string]

DataHub 服务地址，通常以 `http` 或 `https` 开头。

### accessId [string]

访问 DataHub 使用的阿里云 Access ID。

### accessKey [string]

访问 DataHub 使用的阿里云 Access Key。

### project [string]

DataHub 项目名称。

### topic [string]

DataHub Topic 名称。多表写入时可以使用占位符，例如 `${table}`。
`${table_name}` 仅作为已废弃的兼容别名保留，新任务建议使用 `${table}`。

SeaTunnel 字段名需要和 DataHub Topic 中的字段名一致，因为 sink 会按照 Topic 结构里的字段名写入数据。

### timeout [int]

客户端连接最大超时时间，单位为毫秒。

### retryTimes [int]

写入记录失败时的最大重试次数。

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。
多表写入时，可以配合通用参数中的 `multi_table_sink_replica` 使用。

## 任务示例

### 单表写入单个 Topic

将 fake source 的记录写入单个 DataHub topic 的简单批量任务。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  DataHub {
    endpoint = "https://datahub.example.aliyuncs.com"
    accessId = "your-access-id"
    accessKey = "your-access-key"
    project = "demo_project"
    topic = "user_topic"
    timeout = 3000
    retryTimes = 3
  }
}
```

### 多表写入匹配的 Topic

当上游 source 提供多个表时，可以使用 `${table}` 占位符配置 `topic`，让每个输入表路由到同名的 topic。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"

    tables_configs = [
      {
        row.num = 100
        schema = {
          table = "users"
          fields {
            name = "string"
            age = "int"
          }
        }
      },
      {
        row.num = 200
        schema = {
          table = "orders"
          fields {
            order_id = "int"
            amount = "decimal(10, 2)"
          }
        }
      }
    ]
  }
}

sink {
  DataHub {
    endpoint = "https://datahub.example.aliyuncs.com"
    accessId = "your-access-id"
    accessKey = "your-access-key"
    project = "demo_project"
    topic = "${table}"
    timeout = 3000
    retryTimes = 3
  }
}
```

## 变更日志

<ChangeLog />

import ChangeLog from '../changelog/connector-datahub.md';

# DataHub

> DataHub Sink 连接器

## 描述

DataHub Sink 用于将 SeaTunnel 数据写入阿里云 DataHub。

该连接器支持单表写入和多表写入。多表写入时，可以在 `topic` 中使用
`${table_name}` 这类占位符，将不同输入表的数据写入不同的 DataHub Topic。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           | 类型   | 必填 | 默认值 |
|----------------|--------|------|--------|
| endpoint       | string | 是   | -      |
| accessId       | string | 是   | -      |
| accessKey      | string | 是   | -      |
| project        | string | 是   | -      |
| topic          | string | 是   | -      |
| timeout        | int    | 否   | 3000   |
| retryTimes     | int    | 否   | 3      |
| common-options |        | 否   | -      |

### endpoint [string]

DataHub 服务地址，通常以 `http` 或 `https` 开头。

### accessId [string]

访问 DataHub 使用的阿里云 Access ID。

### accessKey [string]

访问 DataHub 使用的阿里云 Access Key。

### project [string]

DataHub 项目名称。

### topic [string]

DataHub Topic 名称。多表写入时可以使用占位符，例如 `${table_name}`。

### timeout [int]

客户端连接最大超时时间，单位为毫秒。

### retryTimes [int]

写入记录失败时的最大重试次数。

### 通用选项

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。
多表写入时，可以配合通用参数中的 `multi_table_sink_replica` 使用。

## 示例

### 单表写入单个 Topic

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
    topic = "${table_name}"
    timeout = 3000
    retryTimes = 3
  }
}
```

## 变更日志

<ChangeLog />

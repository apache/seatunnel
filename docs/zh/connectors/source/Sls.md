import ChangeLog from '../changelog/connector-sls.md';

# Sls

> Sls 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义切分](../../introduction/concepts/connector-v2-features.md)

## 描述

Sls source 连接器用于从阿里云日志服务 SLS 读取日志。它支持批处理和流处理任务，并且可以按
SLS shard 并行读取。流处理模式下，SeaTunnel 会在 checkpoint 完成时提交 SLS 游标，任务重启后
可以从已经提交的位置继续读取。

可以用两种方式读取日志：

- 配置 `schema`，把指定的 SLS 日志字段解析成 SeaTunnel 字段。
- 不配置 `schema`，把每条 SLS 日志作为 JSON 字符串写入单个 `content` 字段。

## 支持的数据源信息

使用 Sls 连接器前，需要通过 `install-plugin.sh` 或 Maven 中央仓库获取以下依赖。

| 数据源 | 支持版本      | Maven                                                                             |
|--------|---------------|-----------------------------------------------------------------------------------|
| Sls    | Universal     | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## Source 选项

| 名称                                | 类型                                      | 是否必填 | 默认值                     | 描述                                                                                                           |
|-------------------------------------|-------------------------------------------|----------|----------------------------|----------------------------------------------------------------------------------------------------------------|
| endpoint                            | String                                    | 是       | -                          | 阿里云 SLS 访问地址，例如 `cn-hangzhou.log.aliyuncs.com` 或内网访问地址。                                      |
| project                             | String                                    | 是       | -                          | [阿里云 SLS Project](https://help.aliyun.com/zh/sls/user-guide/manage-a-project)。                              |
| logstore                            | String                                    | 是       | -                          | [阿里云 SLS Logstore](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore)。                            |
| access_key_id                       | String                                    | 是       | -                          | 阿里云 AccessKey ID。                                                                                          |
| access_key_secret                   | String                                    | 是       | -                          | 阿里云 AccessKey Secret。                                                                                      |
| start_mode                          | `earliest`, `group_cursor`, `latest`      | 否       | `group_cursor`             | 初始读取位置。`earliest` 从最早位置读取，`latest` 从最新位置读取，`group_cursor` 使用消费者组已提交的游标。     |
| consumer_group                      | String                                    | 否       | `SeaTunnel-Consumer-Group` | SLS 消费者组名称。不同任务如果需要独立保存读取位置，应该使用不同的消费者组。                                   |
| auto_cursor_reset                   | `begin`, `end`                            | 否       | `end`                      | 当 `start_mode = group_cursor` 但消费者组还没有已提交游标时，使用的初始化位置。                                |
| batch_size                          | Int                                       | 否       | 1000                       | 每次从每个 shard 拉取的最大日志条数。                                                                          |
| partition-discovery.interval-millis | Long                                      | 否       | -1                         | 动态发现 SLS shard 变化的间隔。小于或等于 0 表示不定时发现。                                                   |
| schema                              | Config                                    | 否       | -                          | 用于解析 SLS 日志字段的 SeaTunnel 表结构。不配置时输出单个 `content` 字符串字段，字段值是整条日志的 JSON 内容。 |

## 注意事项

- 配置的 RAM 用户需要有读取目标 project、logstore、shard、消费者组、checkpoint 和日志数据的权限。
- 批处理模式会读取当前分配的 shard 后结束；持续消费日志请使用流处理模式。
- 不要在日志或任务说明里打印 `access_key_secret`。

## 任务示例

### 配置 Schema 读取日志

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Sls {
    endpoint = "cn-hangzhou-intranet.log.aliyuncs.com"
    project = "project1"
    logstore = "logstore1"
    access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxx"
    access_key_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    consumer_group = "seatunnel-sls-demo"
    start_mode = "group_cursor"
    auto_cursor_reset = "begin"
    batch_size = 1000
    schema = {
      fields = {
        id = "int"
        name = "string"
        description = "string"
        weight = "string"
      }
    }
  }
}

sink {
  Console {}
}
```

### 不配置 Schema 读取日志

不配置 `schema` 时，输出结果只有一个 `content` 字段，字段值是由 SLS 日志内容组成的 JSON 字符串。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Sls {
    endpoint = "cn-hangzhou-intranet.log.aliyuncs.com"
    project = "project1"
    logstore = "logstore1"
    access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxx"
    access_key_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  }
}

sink {
  Console {}
}
```

### 周期性发现新 Shard

默认情况下，连接器只在任务启动时枚举一次 shard。把 `partition-discovery.interval-millis`
设置为正值后，任务运行期间新创建的 shard 也会被持续发现。下面的示例每 5 分钟刷新一次 shard 列表：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Sls {
    endpoint = "cn-hangzhou-intranet.log.aliyuncs.com"
    project = "project1"
    logstore = "logstore1"
    access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxx"
    access_key_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    consumer_group = "seatunnel-sls-demo"
    partition-discovery.interval-millis = 300000
    schema = {
      fields = {
        id = "int"
        name = "string"
      }
    }
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />

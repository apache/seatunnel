import ChangeLog from '../changelog/connector-sls.md';

# Sls

> Sls Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

Sls sink 连接器用于把 SeaTunnel 数据写入阿里云日志服务 SLS。每条 SeaTunnel 数据会先序列化为
JSON，然后作为 SLS 日志项写入，日志内容的 key 为 `content`。

## 支持的数据源信息

使用 Sls 连接器前，需要通过 `install-plugin.sh` 或 Maven 中央仓库获取以下依赖。

| 数据源 | 支持版本      | Maven                                                                             |
|--------|---------------|-----------------------------------------------------------------------------------|
| Sls    | Universal     | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## Sink 选项

| 名称              | 类型   | 是否必填 | 默认值             | 描述                                                                                                   |
|-------------------|--------|----------|--------------------|--------------------------------------------------------------------------------------------------------|
| endpoint          | String | 是       | -                  | 阿里云 SLS 访问地址，例如 `cn-hangzhou.log.aliyuncs.com` 或内网访问地址。                              |
| project           | String | 是       | -                  | [阿里云 SLS Project](https://help.aliyun.com/zh/sls/user-guide/manage-a-project)。                      |
| logstore          | String | 是       | -                  | [阿里云 SLS Logstore](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore)。                    |
| access_key_id     | String | 是       | -                  | 阿里云 AccessKey ID。                                                                                  |
| access_key_secret | String | 是       | -                  | 阿里云 AccessKey Secret。                                                                              |
| source            | String | 否       | `SeaTunnel-Source` | 写入 SLS log group 的 source 标记。                                                                    |
| topic             | String | 否       | `SeaTunnel-Topic`  | 写入 SLS log group 的 topic 标记。                                                                     |

## 注意事项

- 配置的 RAM 用户需要有向目标 project 和 logstore 写入日志的权限。
- sink 在收到数据时立即写入，不提供精确一次提交语义。流处理模式下连接器按行写入；checkpoint
  只对下游状态有用，并不能保证 SLS 端的写入语义。
- 每条数据都会被序列化为 JSON，并写入 SLS 日志项 `content` 字段，不会映射到其它日志 key。
- 不要在日志或任务说明里打印 `access_key_secret`。

## 任务示例

### 写入数据到 SLS（批处理）

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
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
  Sls {
    endpoint = "cn-hangzhou-intranet.log.aliyuncs.com"
    project = "project1"
    logstore = "logstore1"
    access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxx"
    access_key_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    source = "seatunnel-demo"
    topic = "fake-source"
  }
}
```

### 写入数据到 SLS（流处理）

流处理模式下，连接器会保持 SLS Producer 的连接持续打开，每来一行数据就写入一条。
可以配置 `checkpoint.interval` 保护下游状态，但需要清楚每条 `PutLogs` 调用互相独立，
重试只在 Producer 会话内进行，不会跨重启。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
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
  Sls {
    endpoint = "cn-hangzhou.log.aliyuncs.com"
    project = "project1"
    logstore = "logstore1"
    access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxx"
    access_key_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    source = "seatunnel-streaming"
    topic = "fake-source"
  }
}
```

## 变更日志

<ChangeLog />

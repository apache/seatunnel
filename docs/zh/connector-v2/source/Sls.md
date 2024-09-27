# Sls

> Sls source connector

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## 描述

从阿里云Sls日志服务中读取数据。

## 支持的数据源信息

为了使用Sls连接器，需要以下依赖关系。
它们可以通过install-plugin.sh或Maven中央存储库下载。

| 数据源 | 支持的版本     | Maven                                                                             |
|-----|-----------|-----------------------------------------------------------------------------------|
| Sls | Universal | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## Source Options

|                Name                 |                    Type                     | Required |         Default          |                                                            Description                                                             |
|-------------------------------------|---------------------------------------------|----------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| project                             | String                                      | Yes      | -                        | [阿里云 Sls 项目](https://help.aliyun.com/zh/sls/user-guide/manage-a-project?spm=a2c4g.11186623.0.0.6f9755ebyfaYSl)                     |
| logstore                            | String                                      | Yes      | -                        | [阿里云 Sls 日志库](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore?spm=a2c4g.11186623.0.0.13137c08nfuiBC)                   |
| endpoint                            | String                                      | Yes      | -                        | [阿里云访问服务点](https://help.aliyun.com/zh/sls/developer-reference/api-sls-2020-12-30-endpoint?spm=a2c4g.11186623.0.0.548945a8UyJULa)   |
| access_key_id                       | String                                      | Yes      | -                        | [阿里云访问用户ID](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479) |
| access_key_secret                   | String                                      | Yes      | -                        | [阿里云访问用户密码](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479) |
| start_mode                          | StartMode[earliest],[group_cursor],[latest] | No       | group_cursor             | 消费者的初始消费模式                                                                                                                         |
| consumer_group                      | String                                      | No       | SeaTunnel-Consumer-Group | Sls消费者组id，用于区分不同的消费者组                                                                                                              |
| auto_cursor_reset                   | CursorMode[begin],[end]                     | No       | end                      | 当消费者组中没有记录读取游标时，初始化读取游标                                                                                                            |
| batch_size                          | Int                                         | No       | 1000                     | 每次从SLS中读取的数据量                                                                                                                      |
| partition-discovery.interval-millis | Long                                        | No       | -1                       | 动态发现主题和分区的间隔                                                                                                                       |

## 任务示例

### 简单示例

> 此示例读取sls的logstore1的数据并将其打印到客户端。如果您尚未安装和部署SeaTunnel，则需要按照安装SeaTunnel中的说明安装和部署SeaTunnel。然后按照[快速启动SeaTunnel引擎](../../Start-v2/locale/Quick-Start SeaTunnel Engine.md)中的说明运行此作业。

[创建RAM用户及授权](https://help.aliyun.com/zh/sls/create-a-ram-user-and-authorize-the-ram-user-to-access-log-service?spm=a2c4g.11186623.0.i4), 请确认RAM用户有足够的权限来读取及管理数据，参考：[RAM自定义授权示例](https://help.aliyun.com/zh/sls/use-custom-policies-to-grant-permissions-to-a-ram-user?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#reference-s3z-m1l-z2b)

```hocon
# Defining the runtime environment
env {
  parallelism = 2
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
  Console {
  }
}
```


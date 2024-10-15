# Sls

> Sls sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## 描述

Sink connector for Aliyun Sls.

从写入数据到阿里云Sls日志服务

为了使用Sls连接器，需要以下依赖关系。
它们可以通过install-plugin.sh或Maven中央存储库下载。

| Datasource | Supported Versions | Maven                                                                             |
|------------|--------------------|-----------------------------------------------------------------------------------|
| Sls        | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## 支持的数据源信息

|                Name                 | Type     | Required | Default           | Description                                                                                                                        |
|-------------------------------------|----------|----------|-------------------|------------------------------------------------------------------------------------------------------------------------------------|
| project                             | String   | Yes      | -                 | [阿里云 Sls 项目](https://help.aliyun.com/zh/sls/user-guide/manage-a-project?spm=a2c4g.11186623.0.0.6f9755ebyfaYSl)                     |
| logstore                            | String   | Yes      | -                 | [阿里云 Sls 日志库](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore?spm=a2c4g.11186623.0.0.13137c08nfuiBC)                   |
| endpoint                            | String   | Yes      | -                 | [阿里云访问服务点](https://help.aliyun.com/zh/sls/developer-reference/api-sls-2020-12-30-endpoint?spm=a2c4g.11186623.0.0.548945a8UyJULa)   |
| access_key_id                       | String   | Yes      | -                 | [阿里云访问用户ID](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479) |
| access_key_secret                   | String   | Yes      | -                 | [阿里云访问用户密码](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479) |
| source                              | String   | No       | SeaTunnel-Source  | 在sls中数据来源标记                                                                                                                        |
| topic                               | String   | No       | SeaTunnel-Topic   | 在sls中数据主题标记                                                                                                                        |

## 任务示例

### 简单示例

> 此示例写入sls的logstore1的数据。如果您尚未安装和部署SeaTunnel，则需要按照安装SeaTunnel中的说明安装和部署SeaTunnel。然后按照[快速启动SeaTunnel引擎](../../Start-v2/locale/Quick-Start SeaTunnel Engine.md)中的说明运行此作业。

[创建RAM用户及授权](https://help.aliyun.com/zh/sls/create-a-ram-user-and-authorize-the-ram-user-to-access-log-service?spm=a2c4g.11186623.0.i4), 请确认RAM用户有足够的权限来读取及管理数据，参考：[RAM自定义授权示例](https://help.aliyun.com/zh/sls/use-custom-policies-to-grant-permissions-to-a-ram-user?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#reference-s3z-m1l-z2b)

```hocon
# Defining the runtime environment
env {
  parallelism = 2
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
    endpoint = "cn-hangzhou-intranet.log.aliyuncs.com"
    project = "project1"
    logstore = "logstore1"
    access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxx"
    access_key_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  }
}
```


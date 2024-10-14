# Sls

> Sls sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Description

Sink connector for Aliyun Sls.

## Supported DataSource Info

In order to use the Sls connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Maven                                                                             |
|------------|--------------------|-----------------------------------------------------------------------------------|
| Sls        | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## Source Options

| Name                                | Type    | Required | Default          | Description                                                                                                                                      |
|-------------------------------------|---------|----------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| project                             | String  | Yes      | -                | [Aliyun Sls Project](https://help.aliyun.com/zh/sls/user-guide/manage-a-project?spm=a2c4g.11186623.0.0.6f9755ebyfaYSl)                           |
| logstore                            | String  | Yes      | -                | [Aliyun Sls Logstore](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore?spm=a2c4g.11186623.0.0.13137c08nfuiBC)                         |
| endpoint                            | String  | Yes      | -                | [Aliyun Access Endpoint](https://help.aliyun.com/zh/sls/developer-reference/api-sls-2020-12-30-endpoint?spm=a2c4g.11186623.0.0.548945a8UyJULa)   |
| access_key_id                       | String  | Yes      | -                | [Aliyun AccessKey ID](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479)     |
| access_key_secret                   | String  | Yes      | -                | [Aliyun AccessKey Secret](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479) |
| source                              | String  | No       | SeaTunnel-Source | Data Source marking in sls                                                                                                                       |
| topic                               | String  | No       | SeaTunnel-Topic  | Data topic marking in sls                                                                                                                        |

## Task Example

### Simple

> This example write data to the sls's logstore1.And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in Install SeaTunnel to install and deploy SeaTunnel. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

[Create RAM user and authorization](https://help.aliyun.com/zh/sls/create-a-ram-user-and-authorize-the-ram-user-to-access-log-service?spm=a2c4g.11186623.0.i4),Please ensure thr ram user have sufficient rights to perform, reference [RAM Custom Authorization Example](https://help.aliyun.com/zh/sls/use-custom-policies-to-grant-permissions-to-a-ram-user?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#reference-s3z-m1l-z2b)

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


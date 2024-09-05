# Sls

> Sls source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Source connector for Aliyun Sls.

## Supported DataSource Info

In order to use the Sls connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Maven                                                                             |
|------------|--------------------|-----------------------------------------------------------------------------------|
| Sls        | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## Source Options

|                Name                 |                    Type                     | Required |         Default          |                                                                   Description                                                                    |
|-------------------------------------|---------------------------------------------|----------|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| project                             | String                                      | Yes      | -                        | [Aliyun Sls Project](https://help.aliyun.com/zh/sls/user-guide/manage-a-project?spm=a2c4g.11186623.0.0.6f9755ebyfaYSl)                           |
| logstore                            | String                                      | Yes      | -                        | [Aliyun Sls Logstore](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore?spm=a2c4g.11186623.0.0.13137c08nfuiBC)                         |
| endpoint                            | String                                      | Yes      | -                        | [Aliyun Access Endpoint](https://help.aliyun.com/zh/sls/developer-reference/api-sls-2020-12-30-endpoint?spm=a2c4g.11186623.0.0.548945a8UyJULa)   |
| access_key_id                       | String                                      | Yes      | -                        | [Aliyun AccessKey ID](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479)     |
| access_key_secret                   | String                                      | Yes      | -                        | [Aliyun AccessKey Secret](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#task-2245479) |
| start_mode                          | StartMode[earliest],[group_cursor],[latest] | No       | group_cursor             | The initial consumption pattern of consumers.                                                                                                    |
| consumer_group                      | String                                      | No       | SeaTunnel-Consumer-Group | Sls consumer group id, used to distinguish different consumer groups.                                                                            |
| auto_cursor_reset                   | CursorMode[begin],[end]                     | No       | end                      | When there is no cursor in the consumer group, cursor initialization occurs                                                                      |
| batch_size                          | Int                                         | No       | 1000                     | The amount of data pulled from SLS each time                                                                                                     |
| partition-discovery.interval-millis | Long                                        | No       | -1                       | The interval for dynamically discovering topics and partitions.                                                                                  |

## Task Example

### Simple

> This example reads the data of sls's logstore1 and prints it to the client.And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in Install SeaTunnel to install and deploy SeaTunnel. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

[Create RAM user and authorization](https://help.aliyun.com/zh/sls/create-a-ram-user-and-authorize-the-ram-user-to-access-log-service?spm=a2c4g.11186623.0.i4),Please ensure thr ram user have sufficient rights to perform, reference [RAM Custom Authorization Example](https://help.aliyun.com/zh/sls/use-custom-policies-to-grant-permissions-to-a-ram-user?spm=a2c4g.11186623.0.0.4a6e4e554CKhSc#reference-s3z-m1l-z2b)

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


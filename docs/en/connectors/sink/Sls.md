import ChangeLog from '../changelog/connector-sls.md';

# Sls

> Sls sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

The Sls sink connector writes SeaTunnel rows to Alibaba Cloud Simple Log Service (SLS). Each
SeaTunnel row is serialized as JSON and written to SLS as a log item whose content key is `content`.

## Supported DataSource Info

To use the Sls connector, download the following dependency by using `install-plugin.sh` or from the
Maven central repository.

| Datasource | Supported Versions | Maven                                                                             |
|------------|--------------------|-----------------------------------------------------------------------------------|
| Sls        | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## Sink Options

| Name              | Type   | Required | Default            | Description                                                                                             |
|-------------------|--------|----------|--------------------|---------------------------------------------------------------------------------------------------------|
| endpoint          | String | Yes      | -                  | Alibaba Cloud SLS endpoint, for example `cn-hangzhou.log.aliyuncs.com` or an intranet endpoint.         |
| project           | String | Yes      | -                  | [Alibaba Cloud SLS project](https://help.aliyun.com/zh/sls/user-guide/manage-a-project).                |
| logstore          | String | Yes      | -                  | [Alibaba Cloud SLS logstore](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore).              |
| access_key_id     | String | Yes      | -                  | Alibaba Cloud AccessKey ID.                                                                             |
| access_key_secret | String | Yes      | -                  | Alibaba Cloud AccessKey secret.                                                                         |
| source            | String | No       | `SeaTunnel-Source` | Source tag written to SLS log groups.                                                                   |
| topic             | String | No       | `SeaTunnel-Topic`  | Topic tag written to SLS log groups.                                                                    |

## Notes

- The configured RAM user must have permission to write logs to the target project and logstore.
- The sink writes data as soon as `write` is called. It does not provide exactly-once commit semantics.
  In streaming mode the connector flushes row by row; rely on checkpointing only for downstream state,
  not for the SLS writes themselves.
- Each row is serialized as a JSON object and stored under the `content` key of an SLS log item. The
  remaining row fields are not mapped to separate log keys.
- Do not print `access_key_secret` in logs or job descriptions.

## Task Example

### Write Rows to SLS (Batch)

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

### Write Rows to SLS (Streaming)

In streaming mode the connector keeps the SLS producer connection open and writes each row as it
arrives. Configure `checkpoint.interval` to make downstream state recoverable, but keep in mind that
each `PutLogs` call is independent and may retry only within the open client session.

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

## Changelog

<ChangeLog />

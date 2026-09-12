import ChangeLog from '../changelog/connector-sls.md';

# Sls

> Sls source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The Sls source connector reads logs from Alibaba Cloud Simple Log Service (SLS). It can run in batch or
streaming jobs and reads SLS shards in parallel. In streaming mode, SeaTunnel stores the SLS cursor
during checkpoint completion, so a restarted job can continue from the committed cursor.

You can read logs in two ways:

- Configure `schema` to parse named SLS log fields into SeaTunnel columns.
- Omit `schema` to read each SLS log as one JSON string in a single `content` column.

## Supported DataSource Info

To use the Sls connector, download the following dependency by using `install-plugin.sh` or from the
Maven central repository.

| Datasource | Supported Versions | Maven                                                                             |
|------------|--------------------|-----------------------------------------------------------------------------------|
| Sls        | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-sls) |

## Source Options

| Name                                | Type                                      | Required | Default                  | Description                                                                                                                                      |
|-------------------------------------|-------------------------------------------|----------|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| endpoint                            | String                                    | Yes      | -                        | Alibaba Cloud SLS endpoint, for example `cn-hangzhou.log.aliyuncs.com` or an intranet endpoint.                                                  |
| project                             | String                                    | Yes      | -                        | [Alibaba Cloud SLS project](https://help.aliyun.com/zh/sls/user-guide/manage-a-project).                                                         |
| logstore                            | String                                    | Yes      | -                        | [Alibaba Cloud SLS logstore](https://help.aliyun.com/zh/sls/user-guide/manage-a-logstore).                                                       |
| access_key_id                       | String                                    | Yes      | -                        | Alibaba Cloud AccessKey ID.                                                                                                                      |
| access_key_secret                   | String                                    | Yes      | -                        | Alibaba Cloud AccessKey secret.                                                                                                                  |
| start_mode                          | `earliest`, `group_cursor`, `latest`      | No       | `group_cursor`           | Initial cursor mode. `earliest` starts from the beginning, `latest` starts from the end, and `group_cursor` uses the consumer group's checkpoint. |
| consumer_group                      | String                                    | No       | `SeaTunnel-Consumer-Group` | SLS consumer group name. Use different values when separate jobs must keep independent cursors.                                                   |
| auto_cursor_reset                   | `begin`, `end`                            | No       | `end`                    | Cursor position used when `start_mode = group_cursor` but the consumer group has no checkpoint yet.                                               |
| batch_size                          | Int                                       | No       | 1000                     | Maximum logs pulled from each shard in one request.                                                                                              |
| partition-discovery.interval-millis | Long                                      | No       | -1                       | Interval for discovering SLS shard changes. A value less than or equal to 0 disables periodic discovery.                                          |
| schema                              | Config                                    | No       | -                        | SeaTunnel schema for parsing SLS log fields. If omitted, the connector outputs one `content` string column containing the full log as JSON.       |

## Notes

- The configured RAM user must have permission to read the target project, logstore, shards, consumer
  groups, checkpoints, and logs.
- In batch mode, the connector reads the currently assigned shards and then finishes. Use streaming mode
  for continuous log consumption.
- Do not print `access_key_secret` in logs or job descriptions.

## Task Examples

### Read Logs with an Explicit Schema

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

### Read Logs Without a Schema

When `schema` is not configured, each output row has one `content` field. The field value is a JSON
string built from the SLS log contents.

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

### Discover New Shards Periodically

By default the connector enumerates shards once when the job starts. Set
`partition-discovery.interval-millis` to a positive value to keep discovering new shards created
while the job is running. The example below refreshes the shard list every five minutes:

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

## Changelog

<ChangeLog />

import ChangeLog from '../changelog/connector-bigquery.md';

# BigQuery

> BigQuery 数据接收器连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)

## 描述

用于 Google Cloud BigQuery 的数据接收器连接器,使用 Storage Write API 实现高性能数据摄取。

## 写入模式语义

### Batch 模式

在 `batch` 模式下，连接器通过 BigQuery Storage Write API 的 pending stream 写入数据。写入 pending stream 的数据在通过 `BatchCommitWriteStreams` 提交之前，对查询不可见。

如果某个 checkpoint 在 pending stream 已 finalize 但尚未 commit 之前失败，连接器在 restore 时不会提交该 stream。这是有意的行为，因为作业可能会从上一个成功的 checkpoint 重新处理相同的数据
如果在 restore 时提交失败 checkpoint 对应的 stream，可能会导致重复数据。相反，恢复后的 writer 会创建新的 pending stream，并重新写入回放的数据。

### Streaming 模式

在 `streaming` 模式下，连接器直接向 BigQuery 写入记录。当配置 `sequence_number_column` 时，该列的值会作为 `_CHANGE_SEQUENCE_NUMBER` 发送给 BigQuery，用于去重。

## 支持的数据源信息

| 数据源     | 支持的版本    | Maven                                                                                  |
|-----------|-------------|----------------------------------------------------------------------------------------|
| BigQuery  | BOM 26.72.0 | [下载](https://mvnrepository.com/artifact/com.google.cloud/google-cloud-bigquery)      |

## 配置选项

| 名称                         | 类型    | 是否必须 | 默认值  | 描述                                                                                                         |
|-----------------------------|---------|---------|---------|--------------------------------------------------------------------------------------------------------------|
| project_id                  | string  | 是      | -       | GCP 项目 ID                                                                                                  |
| dataset_id                  | string  | 是      | -       | BigQuery 数据集 ID                                                                                            |
| table_id                    | string  | 是      | -       | BigQuery 表 ID                                                                                                |
| service_account_key_path    | string  | 否      | -       | GCP 服务账号 JSON 密钥文件路径                                                                                  |
| service_account_key_json    | string  | 否      | -       | 内联 GCP 服务账号 JSON 密钥内容                                                                                 |
| write_mode                  | string  | 否      | batch   | 写入模式。支持的值：`batch` 和 `streaming`                                                                      |
| sequence_number_column      | string  | 否      | -       | 用于 CDC 去重的序列号列名。仅在 `write_mode` 为 `streaming` 时适用                                                |
| batch_size                  | int     | 否      | 1000    | 发送到 BigQuery 之前批量处理的行数                                                                               |
| emulator_host               | string  | 否      | -       | 用于测试的 BigQuery 模拟器主机地址（例如 `localhost:9050`）                                                        |

### 认证选项

您必须提供以下认证方法之一:

1. **service_account_key_path**: 服务账号 JSON 文件路径
2. **service_account_key_json**: 内联 JSON 密钥内容
3. **默认凭据**: 如果未指定上述选项,则使用应用程序默认凭据 (ADC)

#### sequence_number_column

配置 `sequence_number_column` 后，该列的值将作为 `_CHANGE_SEQUENCE_NUMBER` 发送到 BigQuery，从而实现幂等写入。当源端重新传输时，具有相同序列号的行将被 BigQuery 自动去重。

> **注意**
> - `sequence_number_column` 应引用源表中单调递增的列（例如 `updated_at` 的 epoch 毫秒值、`version` 或 `seq_id`）。列值必须是可转换为 `long` 的类型。
> - 要在流式模式下启用精确一次（去重），目标 BigQuery 表必须定义主键。否则，无论序列号如何，BigQuery 都会将每次写入视为追加操作。

## 任务示例

### 简单示例 (使用服务账号文件)

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1000
    schema = {
      fields {
        user_id = "bigint"
        username = "string"
        email = "string"
        created_at = "timestamp"
      }
    }
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "analytics"
    table_id = "user_events"
    service_account_key_path = "/path/to/key.json"
    batch_size = 1000
  }
}
```

### BigQuery 模拟器示例 (测试)

```hocon
sink {
  BigQuery {
    project_id = "test-project"
    dataset_id = "test_dataset"
    table_id = "test_table"
    emulator_host = "localhost:9050"
    batch_size = 100
  }
}
```

### CDC 流式模式（MySQL 到 BigQuery)

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
      parallelism = 1
      server-id = 5652
      username = "st_user_source"
      password = "mysqlpw"
      table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
      url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "cdc_dataset"
    table_id = "orders"
    service_account_key_path = "/path/to/key.json"
    write_mode = "streaming"
    sequence_number_column = "updated_at"
    batch_size = 500
  }
}
```

### 复杂数据类型示例

```hocon
source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        order_id = "bigint"
        customer = {
          name = "string"
          email = "string"
        }
        items = "array<string>"
        metadata = "map<string, string>"
        order_date = "date"
      }
    }
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "orders"
    table_id = "customer_orders"
    service_account_key_path = "/path/to/key.json"
    batch_size = 500
  }
}
```

## 更新日志

<ChangeLog />
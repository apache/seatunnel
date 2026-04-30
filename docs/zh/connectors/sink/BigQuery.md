import ChangeLog from '../changelog/connector-bigquery.md';

# BigQuery

> BigQuery 数据接收器连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md) 仅适用于 batch 模式
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)

## 描述

用于 Google Cloud BigQuery 的数据接收器连接器,使用 Storage Write API 实现高性能数据摄取。

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

### 表选项

目标 BigQuery 表必须已经存在。
连接器会在 writer 初始化时读取已有的表 schema，并且不会自动创建 BigQuery 表。

#### sequence_number_column

`sequence_number_column` 是可选配置。

当配置了 `sequence_number_column` 时，该列的值会作为 `_CHANGE_SEQUENCE_NUMBER` 发送到 BigQuery，用于启用 BigQuery 侧的去重。在 source 重新发送数据时，具有相同 primary key 和相同 sequence number 的行可以由 BigQuery 进行去重。
如果没有配置 `sequence_number_column`，则不会发送 `_CHANGE_SEQUENCE_NUMBER`，BigQuery 也不会执行基于 sequence number 的去重。

> **注意**
> - `sequence_number_column` 应该引用 source 表中单调递增的列，例如以 epoch millis 表示的 `updated_at`、`version` 或 `seq_id`。该列的值必须能够转换为 `long` 类型。
> - 如果要在 streaming 模式下启用 BigQuery 侧的去重，目标 BigQuery 表必须定义 Primary Key。否则，无论是否配置 sequence number，BigQuery 都会将每次写入视为 append 操作。

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

### 测试

该连接器使用 BigQuery Storage Write API。当前本地 BigQuery emulator 不能完整支持该连接器使用的写入路径。
因此，目前应在真实的 BigQuery 环境中测试该连接器。

## 更新日志

<ChangeLog />
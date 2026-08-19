import ChangeLog from '../changelog/connector-bigquery.md';

# BigQuery

> BigQuery 数据接收器连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md) 仅适用于 batch 模式
- [x] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

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
| emulator_host               | string  | 否      | -       | BigQuery emulator 地址，例如 `localhost:9050`。该参数仅用于测试。                                                |
| universe_domain             | string  | 否      | -       | Google Cloud 宇宙域/环境域名，例如主权云 S3NS 环境配置为 `s3nsapis.fr`。                                          |
| schema_save_mode            | enum    | 否      | CREATE_SCHEMA_WHEN_NOT_EXIST | Schema 保存模式。详见下文。                                                                           |
| data_save_mode              | enum    | 否      | APPEND_DATA | Data 保存模式。详见下文。                                                                                 |
| custom_sql                  | string  | 否      | -       | 当 `data_save_mode` 选择 `CUSTOM_PROCESSING` 时，需要填写的自定义 SQL 语句。                                    |
| multi_table_sink_replica    | int     | 否      | -       | Sink 通用参数，用于控制多表运行时每张表的 sink 副本数。                                                          |
| common-options              |         | 否      | -       | Sink 通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。                            |

### 认证参数

生产 BigQuery 任务必须使用下面任意一种认证方式。只有配置 `emulator_host` 做测试时才会跳过认证。

1. **service_account_key_path**：服务账号 JSON 密钥文件路径。
2. **service_account_key_json**：直接填写服务账号 JSON 密钥内容。
3. **默认凭据**：如果前两项都不配置，则使用 Google Application Default Credentials。

### 表选项

目标 BigQuery 表可以通过 SeaTunnel SaveMode 自动创建。
通过将 `schema_save_mode` 配置为 `CREATE_SCHEMA_WHEN_NOT_EXIST`（默认值）或 `RECREATE_SCHEMA`，连接器在初始化时可以基于上游 schema 信息自动创建 BigQuery 数据集和数据表。

连接器写入的目标表由 `project_id.dataset_id.table_id` 决定。
在多表同步场景下，您可以将 `table_id` 配置为包含 `${table_name}` 的表达式（例如 `table_id = "${table_name}"` 或 `table_id = "prefix_${table_name}"`），从而将数据动态路由到不同的 BigQuery 表中。在这种多表设置下，连接器将根据上游表信息自动在 BigQuery 中创建相应的目标表。

### schema_save_mode [Enum]

在同步任务启动之前，控制如何处理目标表的结构。
- `RECREATE_SCHEMA` ：如果目标表存在，则先删除该表然后重新创建；如果不存在则直接创建。
- `CREATE_SCHEMA_WHEN_NOT_EXIST` ：如果目标表不存在则创建它；如果已存在则跳过创建。
- `ERROR_WHEN_SCHEMA_NOT_EXIST` ：如果目标表不存在，则抛出异常并报错。
- `IGNORE` ：忽略目标表结构的处理，不执行任何与结构相关的检查或 DDL 动作。

### data_save_mode [Enum]

在同步任务启动之前，控制如何处理目标表中的已有数据。
- `DROP_DATA` ：删除目标表中的已有数据。
- `APPEND_DATA` ：保留目标表中的已有数据，并将新数据追加写入。
- `CUSTOM_PROCESSING` ：执行用户自定义的处理。此选项需要配合 `custom_sql` 参数。
- `ERROR_WHEN_DATA_EXISTS` ：如果目标表已包含数据，则抛出异常并报错。

### custom_sql [String]

当 `data_save_mode` 被设置为 `CUSTOM_PROCESSING` 时，此参数中填写的自定义 SQL 语句将在数据开始写入前被执行。

### 写入模式

- `batch`：使用 BigQuery buffered write stream，并在 SeaTunnel checkpoint/commit 阶段提交数据。主要特性中的精确一次能力指的是该模式。
- `streaming`：使用默认 stream，并携带 BigQuery change 字段写入 CDC 记录。该模式适合 CDC 的 upsert/delete 数据，但该连接器没有将它标记为精确一次。

使用 `streaming` 模式写入 CDC 数据时，请先在 BigQuery 中创建好带 Primary Key 的目标表。连接器会把 SeaTunnel 的行类型转换为 BigQuery change 记录：`INSERT` 和 `UPDATE_AFTER` 会写成 `UPSERT`，`DELETE` 和 `UPDATE_BEFORE` 会写成 `DELETE`。

#### sequence_number_column

`sequence_number_column` 是可选配置。

当配置了 `sequence_number_column` 时，该列的值会作为 `_CHANGE_SEQUENCE_NUMBER` 发送到 BigQuery，用于启用 BigQuery 侧的去重。在 source 重新发送数据时，具有相同 primary key 和相同 sequence number 的行可以由 BigQuery 进行去重。
如果没有配置 `sequence_number_column`，则不会发送 `_CHANGE_SEQUENCE_NUMBER`，BigQuery 也不会执行基于 sequence number 的去重。

> **注意**
> - BigQuery 要求 `_CHANGE_SEQUENCE_NUMBER` 是十六进制 `STRING`。对于整数列以及精确的整数 decimal 值（例如映射为 `DECIMAL(20, 0)` 的 MySQL `BIGINT UNSIGNED`），connector 会将 unsigned 64-bit 范围内的非负值转换为十六进制字符串；对于字符串列，connector 会将值视为已编码的十六进制 sequence number，仅进行校验而不转换。
> - sequence number 最多可以包含 4 个以 `/` 分隔的 section，每个 section 最多包含 16 个十六进制字符。Null、负数、空值或格式错误的值会被拒绝。
> - `sequence_number_column` 应该引用 source 表中单调递增的列，例如以 epoch millis 表示的 `updated_at`、`version` 或 `seq_id`。
> - 如果要在 streaming 模式下启用 BigQuery 侧的去重，目标 BigQuery 表必须定义 Primary Key。否则，无论是否配置 sequence number，BigQuery 都会将每次写入视为 append 操作。

### emulator_host

`emulator_host` 只用于本地测试或 CI 测试。配置该参数后，SeaTunnel 会无凭据连接 BigQuery emulator。生产任务不要使用该参数。

## 任务示例

### 简单批处理示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    string.fake.mode = "template"
    string.template = ["key", "value"]
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
        c_time = time
      }
    }
  }
}

sink {
  BigQuery {
    project_id = "test-project"
    dataset_id = "test_dataset"
    table_id = "test_table"
    batch_size = 2
    emulator_host = "localhost:9050"
  }
}
```

### CDC 流式模式（MySQL 到 BigQuery)

目标 BigQuery 表需要提前创建，并且应定义 CDC 源表使用的主键。例如：

```sql
CREATE TABLE `my-gcp-project.cdc_dataset.orders` (
  uuid INT64 NOT NULL,
  name STRING,
  score INT64,
  PRIMARY KEY (uuid) NOT ENFORCED
)
OPTIONS (max_staleness = INTERVAL 0 MINUTE);
```

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
    batch_size = 500
  }
}
```

如果上游 CDC 源能产生单调递增的列（例如 `updated_at` 毫秒时间戳或行版本号），可以把它配到 `sequence_number_column`，让 BigQuery 端对重试批次做去重。目标表必须定义主键（上例使用 `PRIMARY KEY (uuid) NOT ENFORCED`），否则 BigQuery 会把每次写入都当作 append，跳过去重。

```hocon
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

### 内联服务账号密钥

如果不便挂载密钥文件（例如 CI runner、把密钥放在 Kubernetes Secret 中以环境变量形式注入），可以直接把 JSON 内容放到 `service_account_key_json` 中。

```hocon
sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "orders"
    table_id = "customer_orders"
    service_account_key_json = "${GCP_SA_KEY_JSON}"
    batch_size = 500
  }
}
```

### 测试

该连接器使用 BigQuery Storage Write API。当前本地 BigQuery emulator 不能完整支持该连接器使用的写入路径。
`emulator_host` 只适合用于本地或 CI 中与 emulator 兼容的检查。生产可用性验证应在真实 BigQuery 环境中完成。

## 更新日志

<ChangeLog />

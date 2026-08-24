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
- [x] Schema 演进（仅支持 `ADD COLUMN`）
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
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
| schema_evolution_enabled    | boolean | 否      | false   | 是否将 `ADD COLUMN` Schema 变更事件应用到目标 BigQuery 表                                                        |
| schema_evolution_relax_not_null | boolean | 否   | false   | Schema 演进时是否将源端非空列创建为 BigQuery `NULLABLE` 字段                                                     |
| batch_size                  | int     | 否      | 1000    | 发送到 BigQuery 之前批量处理的行数                                                                               |
| emulator_host               | string  | 否      | -       | BigQuery emulator REST 地址，例如 `localhost:9050`。该参数仅用于测试。                                           |
| emulator_grpc_host          | string  | 否      | -       | BigQuery emulator Storage Write API 地址，例如 `localhost:9060`；默认回退到 `emulator_host`。仅用于测试。          |
| multi_table_sink_replica    | int     | 否      | -       | Sink 通用参数，用于控制多表运行时每张表的 sink 副本数；但该连接器仍只写入配置中的单个 BigQuery 表。                    |
| common-options              |         | 否      | -       | Sink 通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。                            |

### 认证参数

生产 BigQuery 任务必须使用下面任意一种认证方式。只有配置 `emulator_host` 做测试时才会跳过认证。

1. **service_account_key_path**：服务账号 JSON 密钥文件路径。
2. **service_account_key_json**：直接填写服务账号 JSON 密钥内容。
3. **默认凭据**：如果前两项都不配置，则使用 Google Application Default Credentials。

### 表选项

目标 BigQuery 表必须已经存在。
连接器会在 writer 初始化时读取已有的表 schema，并且不会自动创建 BigQuery 表。

该连接器会写入一个固定的目标表：`project_id.dataset_id.table_id`。它不会按上游表自动创建或切换 BigQuery 目标表。如果任务里有多张表，请配置多个 BigQuery sink，或者在写入 BigQuery 前先完成表路由。

### Schema 演进

Schema 演进默认关闭。需要在 BigQuery sink 中设置 `schema_evolution_enabled = true`，并在支持的 CDC source 中设置 `schema-changes.enabled = true`，才能将源表的 `ADD COLUMN` 事件同步到配置的目标表。

仅支持物理列的 `ADD COLUMN` 事件。默认情况下，新增的标量列或 struct 列必须允许为空。设置 `schema_evolution_relax_not_null = true` 后，源端的非空标量列或 struct 列会在 BigQuery 中创建为 `NULLABLE` 字段；这是因为目标表中的历史数据没有新列对应的值。

源端 array 列必须是非空列，并会创建为 BigQuery `REPEATED` 字段。nullable array 会被拒绝，因为 BigQuery array 不能为 `NULL`；静默映射会丢失 `NULL` 与空数组之间的区别。不支持 `DROP COLUMN`、`RENAME COLUMN` 和 `MODIFY COLUMN`。BigQuery 会把新字段追加到目标 Schema 末尾，因此源事件中的 `FIRST` 和 `AFTER` 位置提示不会改变 BigQuery 的物理字段顺序。数据行按字段名编码，sink 会在接收使用新字段的数据前刷新 writer Schema。

遇到不支持的 Schema 变更时，任务会失败而不会静默跳过，因为在源端和目标端 Schema 不一致的情况下继续运行可能导致后续数据错位或损坏。从同一个 checkpoint 恢复可能会再次回放该事件并重复失败。重新启动前，请先协调源表与 BigQuery 表的 Schema，然后从不会再次回放该事件的 source 位置启动。如果数据链路可能产生不支持的 DDL，请关闭 `schema-changes.enabled`，并在 SeaTunnel 外部管理这些 Schema 变更。

Schema 更新使用 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`。如果目标表已经存在同名字段，其类型和模式必须兼容，否则任务会失败。除 Storage Write API 写入所需权限外，凭据还必须能够执行 DDL job 并读取更新后的表元数据。

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

`emulator_host` 只用于本地测试或 CI 测试，用于配置 emulator 的 REST 地址。配置后，SeaTunnel 会无凭据连接 BigQuery emulator。当 emulator 的 Storage Write API 使用不同地址时，需要设置 `emulator_grpc_host`；例如 goccy BigQuery emulator 默认使用 `9060` 端口。未配置时，gRPC 地址会回退到 `emulator_host`。生产任务不要使用这些参数。

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
    emulator_grpc_host = "localhost:9060"
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
      schema-changes.enabled = true
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "cdc_dataset"
    table_id = "orders"
    service_account_key_path = "/path/to/key.json"
    write_mode = "streaming"
    schema_evolution_enabled = true
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

该连接器同时使用 BigQuery REST API 和 Storage Write API。使用 goccy BigQuery emulator 时，请将 `emulator_host` 配置为 REST 端口（默认 `9050`），并将 `emulator_grpc_host` 配置为 gRPC 端口（默认 `9060`）。
Emulator 适合用于本地和 CI 覆盖，但生产可用性仍应在真实 BigQuery 环境中验证。

## 更新日志

<ChangeLog />

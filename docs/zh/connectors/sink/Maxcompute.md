import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute 接收器连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于向 Maxcompute 写入数据。连接器支持 AccessKey（`accessId`/`accesskey`）认证、STS Token 临时认证以及阿里云默认凭据链免密认证；支持追加写入、覆盖整表或分区、自动创建目标表（基于 DDL 模板）以及通过 `insert_strategy` 选择 upload 或 upsert 会话。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持 CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                    | 类型    | 必须 | 默认值                       | 说明                                                                                              |
|---------------------------|---------|------|------------------------------|---------------------------------------------------------------------------------------------------|
| accessId                  | string  | 否   | -                            | 访问 MaxCompute 的 AccessKey ID。                                                                  |
| accesskey                 | string  | 否   | -                            | 访问 MaxCompute 的 AccessKey Secret。                                                              |
| sts_token                 | string  | 否   | -                            | MaxCompute 临时认证 STS Token；配置 `sts_token` 时 `accessId` 与 `accesskey` 必填。                  |
| endpoint                  | string  | 是   | -                            | MaxCompute 端点，以 `http` 开头。                                                                  |
| project                   | string  | 是   | -                            | 在阿里云中创建的 MaxCompute 项目。                                                                  |
| table_name                | string  | 是   | -                            | 目标 MaxCompute 表名，例如 `fake`。                                                                |
| schema_name               | string  | 否   | -                            | MaxCompute Schema 名称；仅当表位于非默认 Schema 时需要设置。                                       |
| partition_spec            | string  | 否   | -                            | MaxCompute 分区表的规范，例如 `ds='20220101'`。                                                    |
| overwrite                 | boolean | 否   | false                        | 是否覆盖整张表或单个分区。                                                                          |
| schema_save_mode          | enum    | 否   | CREATE_SCHEMA_WHEN_NOT_EXIST | 写入前如何处理目标表结构，例如 `RECREATE_SCHEMA` 或 `CREATE_SCHEMA_WHEN_NOT_EXIST`。                |
| data_save_mode            | enum    | 否   | APPEND_DATA                  | 写入前如何处理已有数据，例如 `DROP_DATA`、`APPEND_DATA`、`ERROR_WHEN_DATA_EXISTS`。                |
| custom_sql                | string  | 否   | -                            | 当 `data_save_mode = CUSTOM_PROCESSING` 时执行的 SQL。                                              |
| save_mode_create_template | string  | 否   | 见下文                       | 在 sink 自动建表时使用的 DDL 模板。                                                                  |
| datetime_format           | string  | 否   | yyyy-MM-dd HH:mm:ss          | 将 `LocalDateTime` 字段序列化为字符串时使用的格式。                                                  |
| tunnel_endpoint           | string  | 否   | -                            | MaxCompute Tunnel 服务的自定义端点；未配置时根据区域自动推断。                                       |
| tunnel_name               | string  | 否   | -                            | Tunnel Quota 名称；需同时将 `endpoint` 与 `tunnel_endpoint` 配置为 VPC 端点。                       |
| insert_strategy           | string  | 否   | upload                       | 插入会话类型：`upload` 使用 upload 会话，`upsert` 使用 upsert 会话并要求目标表存在主键。            |
| multi_table_sink_replica  | int     | 否   | 1                            | 多表写入时每张表对应的 Sink Writer 副本数。                                                         |
| common-options            |         | 否   | -                            | Sink 插件通用参数，例如 `plugin_input`。                                                            |

### accessId [string]

`accessId` 您的 Maxcompute accessId，可从阿里云访问。

### accesskey [string]

`accesskey` 您的 Maxcompute accessKey，可从阿里云访问。

### sts_token [string]

`sts_token` 您的 MaxCompute STS Token，用于临时认证。 **注意：** 如果提供了 `sts_token`，则必须同时提供 `accessId` 和 `accesskey`。

> **免密认证 (ECS RAM Role, 环境变量等)**
> 要使用免密认证，只需将 `accessId`、`accesskey` 和 `sts_token` 全部留空不填。连接器将自动回退到阿里云默认凭据链 (DefaultCredentialsProvider) 读取凭证（包括环境变量、系统属性、CLI 配置文件、OIDC 以及 ECS RAM 角色）。

### endpoint [string]

`endpoint` 您的 Maxcompute endpoint，以 http 开头。

### project [string]

`project` 您在阿里云中创建的 Maxcompute 项目。

### table_name [string]

`table_name` 目标 Maxcompute 表名，例如：fake。

### partition_spec [string]

`partition_spec` Maxcompute 分区表的规范，例如：ds='20220101'。

### schema_name [string]

`schema_name` MaxCompute Schema 名称（Project 与 Table 之间的命名空间）。
仅当表位于 MaxCompute 项目的**非默认 Schema** 时才需要设置。
参见 [Schema 相关操作](https://help.aliyun.com/zh/maxcompute/user-guide/schema-related-operations)。

默认值：不设置（使用项目默认 Schema）。

### overwrite [boolean]

`overwrite` 是否覆盖表或分区，默认值：false。

### save_mode_create_template

我们使用模板来自动创建 MaxCompute 表，
它将根据上游数据和模式类型的类型创建相应的表创建语句，
默认模板可以根据情况进行修改。目前仅在多表模式下工作。

默认模板：

```sql
CREATE TABLE IF NOT EXISTS `${table}` (
${rowtype_fields}
) COMMENT '${comment}';
```

如果在模板中填入自定义字段，例如添加 `id` 字段

```sql
CREATE TABLE IF NOT EXISTS `${table}`
(   
    id,
    ${rowtype_fields}
) COMMENT '${comment}';
```

连接器将自动从上游获取相应的类型来完成填充，
并从 `rowtype_fields` 中删除 id 字段。此方法可用于自定义修改字段类型和属性。

您可以使用以下占位符

- database：用于获取上游模式中的数据库
- table_name：用于获取上游模式中的表名
- rowtype_fields：用于获取上游模式中的所有字段，我们将自动映射到 MaxCompute 的字段描述
- rowtype_primary_key：用于获取上游模式中的主键（可能是列表）
- rowtype_unique_key：用于获取上游模式中的唯一键（可能是列表）
- comment：用于获取上游模式中的表注释

### schema_save_mode [Enum]

在同步任务打开之前，为目标端现有的表结构选择不同的处理方案。  
选项介绍：  
`RECREATE_SCHEMA` ：表不存在时将创建，表已保存时删除并重建。如果设置了 `partition_spec`，分区将被删除并重建。        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：表不存在时将创建，表已保存时跳过。如果设置了 `partition_spec`，分区将被创建。        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：表不存在时将报错  
`IGNORE` ：忽略表的处理

### data_save_mode [Enum]

在同步任务打开之前，为目标端现有的数据选择不同的处理方案。  
选项介绍：  
`DROP_DATA`：保留数据库结构并删除数据  
`APPEND_DATA`：保留数据库结构，保留数据  
`CUSTOM_PROCESSING`：用户定义的处理  
`ERROR_WHEN_DATA_EXISTS`：当存在数据时，报错

### custom_sql [String]

当 data_save_mode 选择 CUSTOM_PROCESSING 时，您应该填入 CUSTOM_SQL 参数。此参数通常填入可以执行的 SQL。SQL 将在同步任务之前执行。

### datetime_format [String]

用户定义的格式字符串，用于将 LocalDateTime 字段转换为字符串。

当您想指定与 DateTimeUtils.Formatter 中的预定义值之一匹配的自定义日期时间格式时，请使用此选项（例如 yyyy-MM-dd HH:mm:ss、yyyyMMddHHmmss 等）。

示例值：

- `yyyy-MM-dd HH:mm:ss`
- `yyyy-MM-dd HH:mm:ss.SSSSSS`
- `yyyy.MM.dd HH:mm:ss`
- `yyyy/MM/dd HH:mm:ss`
- `yyyy/M/d HH:mm`
- `yyyy-M-d HH:mm`
- `yyyy/M/d HH:mm:ss`
- `yyyy-M-d HH:mm:ss`
- `yyyyMMddHHmmss`

默认值：`yyyy-MM-dd HH:mm:ss`

### tunnel_endpoint [String]
指定 MaxCompute Tunnel 服务的自定义端点 URL。

默认情况下，端点是从配置的区域自动推断的。

此选项允许您覆盖默认行为并使用自定义 Tunnel 端点。
如果未指定，连接器将使用基于区域的默认 Tunnel 端点。

通常，您**不需要**设置 tunnel_endpoint。仅在自定义网络、调试或本地开发时才需要。

示例值：

- `https://dt.cn-hangzhou.maxcompute.aliyun.com`
- `https://dt.ap-southeast-1.maxcompute.aliyun.com`
- `http://maxcompute:8080`

默认值：未设置（从区域自动推断）

### tunnel_name [String]

`tunnel_name` 指定 Tunnel Quota 名称，用于独占资源组。

Tunnel Quota 允许您使用专用的计算资源进行 MaxCompute Tunnel 数据传输，从而提供更好的性能和资源隔离。

**重要提示**：Tunnel Quota 仅在 **VPC（虚拟私有云）端点**下生效，暂不支持公共网络访问。使用 `tunnel_name` 时，必须同时配置 `endpoint` 和 `tunnel_endpoint` 为 VPC 端点。

如果未指定，将使用默认的 Tunnel quota。

示例值：

- `your_tunnel_quota_name`

默认值：未设置（使用默认 quota）

### insert_strategy [string]

如果将 `insert_strategy` 设置为 `upload`，插入操作将使用 upload 会话。
如果设置为 `upsert`，插入操作将使用 upsert 会话。Upsert 会话 需要主键。

注意：
在同时存在更新或删除操作的情况下，使用 upload 会话进行插入操作，可能会导致插入的记录 比预期更晚出现在表中。
当表中存在主键时，建议将 `insert_strategy` 设置为 `upsert`，以确保一致的 upsert 行为。

`UPDATE_AFTER` 和 `DELETE` 数据都会通过 MaxCompute upsert 会话写入，所以任务包含更新或删除数据时，目标表必须有主键。当前 Sink 不支持 `UPDATE_BEFORE` 数据。

### multi_table_sink_replica [int]

多表写入模式下的 writer 副本数，默认值为 `1`。

当上游数据包含多张表，并且 `table_name` 使用 `${table_name}` 这类占位符时可以配置该参数。例如 `table_name = "${table_name}_sink"` 会把上游表 `test_table` 写入目标表 `test_table_sink`。

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md) 详见。

## 示例

### 追加写入

```hocon
sink {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>"
    table_name="<your table name>"
    #partition_spec="<your partition spec>"
    #overwrite = false
  }
}
```

### 多表写入

```hocon
source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "test_table"
          fields {
            ID = int
            NAME = string
            AGE = int
          }
          primaryKey {
            name = "ID"
            columnNames = [ID]
          }
        }
        rows = [
          { kind = INSERT, fields = [1, "INSERT_TEST1", 20] }
          { kind = INSERT, fields = [2, "INSERT_TEST2", 30] }
        ]
      },
      {
        schema = {
          table = "test_table_2"
          fields {
            ID = int
            NAME = string
            AGE = int
          }
          primaryKey {
            name = "ID"
            columnNames = [ID]
          }
        }
        rows = [
          { kind = INSERT, fields = [1, "INSERT_TEST1", 20] }
        ]
      }
    ]
  }
}

sink {
  Maxcompute {
    accessId = "ak"
    accesskey = "sk"
    endpoint = "http://maxcompute:8080"
    tunnel_endpoint = "http://maxcompute:8080"
    project = "mocked_mc"
    table_name = "${table_name}_sink"
    insert_strategy = "upsert"
    multi_table_sink_replica = 1
  }
}
```

### 更新插入或删除数据

当上游表结构有主键，并且任务里包含更新或删除数据时，建议配置 `insert_strategy = "upsert"`。

```hocon
source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "test_table_sink"
          fields {
            ID = int
            NAME = string
            AGE = int
          }
          primaryKey {
            name = "ID"
            columnNames = [ID]
          }
        }
        rows = [
          {
            kind = UPDATE_AFTER
            fields = [1, "UPSERT_TEST", 100]
          }
        ]
      }
    ]
  }
}

sink {
  Maxcompute {
    accessId = "ak"
    accesskey = "sk"
    endpoint = "http://maxcompute:8080"
    tunnel_endpoint = "http://maxcompute:8080"
    project = "mocked_mc"
    table_name = "test_table_sink"
    insert_strategy = "upsert"
  }
}
```

## 变更日志

<ChangeLog />

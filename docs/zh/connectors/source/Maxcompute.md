import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute 源连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 Maxcompute 读取数据。连接器支持 AccessKey（`accessId`/`accesskey`）认证、STS Token 临时认证，以及阿里云默认凭据链（环境变量、ECS RAM 角色等）免密认证；支持通过 `table_name` 读取单表、通过 `table_list` 读取多表，并支持分区表、自定义列和并行读取所需的 `split_row`。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称            | 类型   | 必需                               | 默认值 | 说明                                                                                                        |
|-----------------|--------|------------------------------------|--------|-------------------------------------------------------------------------------------------------------------|
| accessId        | string | 否                                 | -      | 访问 MaxCompute 的 AccessKey ID。                                                                            |
| accesskey       | string | 否                                 | -      | 访问 MaxCompute 的 AccessKey Secret。                                                                       |
| sts_token       | string | 否                                 | -      | MaxCompute 临时认证 STS Token。配置 `sts_token` 时，`accessId` 与 `accesskey` 必填。                          |
| endpoint        | string | 是                                 | -      | MaxCompute 端点，以 `http` 开头。                                                                            |
| project         | string | 是                                 | -      | 在阿里云中创建的 MaxCompute 项目。                                                                           |
| table_name      | string | 未配置 `table_list` 时必填          | -      | 目标 MaxCompute 表名，例如 `fake`。                                                                          |
| schema_name     | string | 否                                 | -      | MaxCompute Schema 名称（Project 与 Table 之间的命名空间）。仅当表位于非默认 Schema 时需要设置。               |
| partition_spec  | string | 否                                 | -      | MaxCompute 分区表的规范，例如 `ds='20220101'`。                                                              |
| split_row       | int    | 否                                 | 10000  | 每个 split 包含的行数。                                                                                       |
| read_columns    | Array  | 否                                 | -      | 要读取的列；不设置时读取全部列，例如 `["col1", "col2"]`。                                                    |
| table_list      | Array  | 否                                 | -      | 要读取的表列表；可替代 `table_name` 一次读取多张 MaxCompute 表。                                              |
| tunnel_endpoint | string | 否                                 | -      | MaxCompute Tunnel 服务的自定义端点；未配置时根据区域自动推断。                                                |
| tunnel_name     | string | 否                                 | -      | Tunnel Quota 名称；需同时将 `endpoint` 与 `tunnel_endpoint` 配置为 VPC 端点。                                |
| schema          | config | 否                                 | -      | 源表结构；未配置 `read_columns` 时按 schema 中字段读取。                                                       |
| common-options  |        | 否                                 | -      | Source 插件通用参数，例如 `plugin_output`。                                                                  |

### accessId [string]

`accessId` 您的 Maxcompute 密钥 Id.

### accesskey [string]

`accesskey` 您的 Maxcompute 密钥.

### sts_token [string]

`sts_token` 您的 MaxCompute STS Token，用于临时认证。 **注意：** 如果提供了 `sts_token`，则必须同时提供 `accessId` 和 `accesskey`。

> **免密认证 (ECS RAM Role, 环境变量等)**
> 要使用免密认证，只需将 `accessId`、`accesskey` 和 `sts_token` 全部留空不填。连接器将自动回退到阿里云默认凭据链 (DefaultCredentialsProvider) 读取凭证（包括环境变量、系统属性、CLI 配置文件、OIDC 以及 ECS RAM 角色）。

### endpoint [string]

`endpoint` 您的 Maxcompute 端点，以 http 开头。

### project [string]

`project` 您在阿里云中创建的 Maxcompute 项目。

### table_name [string]

`table_name` 目标 Maxcompute 表名，例如：`fake`。

`table_name` 和 `table_list` 不能同时配置。读取单表时使用 `table_name`，读取多表时使用 `table_list`。

### partition_spec [string]

`partition_spec` Maxcompute 分区表的规范，例如: ds='20220101'。

### schema_name [string]

`schema_name` MaxCompute Schema 名称（Project 与 Table 之间的命名空间）。
仅当表位于 MaxCompute 项目的**非默认 Schema** 时才需要设置。
参见 [Schema 相关操作](https://help.aliyun.com/zh/maxcompute/user-guide/schema-related-operations)。

使用 `table_list` 时，每个条目可以单独指定 `schema_name`，会覆盖顶层的值。

默认值：不设置（使用项目默认 Schema）。

### split_row [int]

`split_row` 每次拆分的行数，默认值: 10000.

### read_columns [Array]

`read_columns` 要读取的列，如果未设置，则将读取所有列。例如. ["col1", "col2"]

### table_list [Array]

要读取的表列表，您可以使用此配置代替 `table_name`。

每个表配置项都必须包含 `table_name`，也可以单独覆盖 `project`、`schema_name`、`partition_spec`、`split_row` 和 `read_columns`。如果表配置项没有设置这些值，连接器会使用顶层配置。

当一个任务需要用同一组账号、endpoint 和默认 project 读取多张 MaxCompute 表时，可以使用该模式。

### tunnel_endpoint [String]

MaxCompute Tunnel 服务的自定义端点。未配置时，连接器会根据区域自动推断默认 Tunnel 端点。
一般只有自定义网络、调试或本地开发时才需要配置，例如 `http://maxcompute:8080`。

通常，您**不需要**设置 `tunnel_endpoint`。仅在自定义网络、调试或本地开发时才需要。

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

### common options

源插件常用参数, 详见 [源通用选项](../common-options/source-common-options.md) .

## 示例

### 表读取

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>"
    table_name="<your table name>"
    #tunnel_endpoint="<your tunnel endpoint>"
    #partition_spec="<your partition spec>"
    #split_row = 10000
    #read_columns = ["col1", "col2"]
  }
}
```

### 使用表列表读取

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>" # default project
    #tunnel_endpoint="<your tunnel endpoint>"
    table_list = [
      {
        table_name = "test_table"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      },
      {
        project = "test_project"
        table_name = "test_table2"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      }
    ]
  }
}
```

## 变更日志

<ChangeLog />

import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute 源连接器

## 描述

用于从 Maxcompute 读取数据.

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           |  类型  | 必需 | 默认值 |
|----------------|--------|----|---------------|
| accessId       | string | 否  | -             |
| accesskey      | string | 否  | -             |
| sts_token      | string | 否  | -             |
| endpoint       | string | 是  | -             |
| project        | string | 是  | -             |
| table_name     | string | 是  | -             |
| schema_name    | string | 否  | -             |
| partition_spec | string | 否  | -             |
| split_row      | int    | 否 | 10000         |
| read_columns   | Array  | 否 | -             |
| table_list     | Array  | 否 | -             |
| common-options | string | 否 |               |
| schema         | config | 否 |               |

### accessId [string]

`accessId` 您的 Maxcompute 密钥 Id.

### accesskey [string]

`accesskey` 您的 Maxcompute 密钥.

### sts_token [string]

`sts_token` 您的 MaxCompute STS Token，用于临时认证。 **注意：** 如果提供了 `sts_token`，则必须同时提供 `accessId` 和 `accesskey`。

> **免密认证 (ECS RAM Role, 环境变量等)**
> 要使用免密认证，只需将 `accessId`、`accesskey` 和 `sts_token` 全部留空不填。连接器将自动回退到阿里云默认凭据链 (DefaultCredentialsProvider) 读取凭证（包括环境变量、系统属性、CLI 配置文件、OIDC 以及 ECS RAM 角色）。

### endpoint [string]

`endpoint` 您的 Maxcompute 端点以 http 开头.

### project [string]

`project` 您在阿里云中创建的Maxcompute项目.

### table_name [string]

`table_name` 目标Maxcompute表名，例如：fake.

### partition_spec [string]

`partition_spec` Maxcompute分区表的此规范，例如:ds='20220101'.

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

要读取的表列表，您可以使用此配置代替 `table_name`.

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
import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute Sink 连接器

## 描述

用于从 Maxcompute 读取数据。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 选项

|      参数名      |  类型   | 必须 | 默认值 |
|----------------|---------|------|--------|
| accessId       | string  | 是   | -      |
| accesskey      | string  | 是   | -      |
| endpoint       | string  | 是   | -      |
| project        | string  | 是   | -      |
| table_name     | string  | 是   | -      |
| partition_spec | string  | 否   | -      |
| overwrite      | boolean | 否   | false  |
| common-options | string  | 否   |        |

### accessId [string]

`accessId` 您的 Maxcompute accessId，可从阿里云访问。

### accesskey [string]

`accesskey` 您的 Maxcompute accessKey，可从阿里云访问。

### endpoint [string]

`endpoint` 您的 Maxcompute endpoint，以 http 开头。

### project [string]

`project` 您在阿里云中创建的 Maxcompute 项目。

### table_name [string]

`table_name` 目标 Maxcompute 表名，例如：fake。

### partition_spec [string]

`partition_spec` Maxcompute 分区表的规范，例如：ds='20220101'。

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

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md) 详见。

## 示例

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

## 变更日志

<ChangeLog />



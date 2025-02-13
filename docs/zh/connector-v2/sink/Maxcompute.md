# Maxcompute

> Maxcompute接收器连接器

## 描述

用于从Maxcompute读取数据。

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 选项

|      名称      |  类型   | 需要 | 默认 值 |
|----------------|---------|----------|---------------|
| accessId       | string  | 是      | -             |
| accesskey      | string  | 是      | -             |
| endpoint       | string  | 是      | -             |
| project        | string  | 是      | -             |
| table_name     | string  | 是      | -             |
| partition_spec | string  | 否       | -             |
| overwrite      | boolean | 否       | false         |
| common-options | string  | 否       |               |

### accessId [string]

`accessId` 您的Maxcompute accessId可以从阿里云访问哪个云。

### accesskey [string]

`accesskey` 您的Maxcompute accessKey可以从阿里云访问哪个云。

### endpoint [string]

`endpoint` 您的Maxcompute端点以http开头。

### project [string]

`project` 您在阿里云中创建的Maxcompute项目。

### table_name [string]

`table_name` 目标最大计算表名，例如：fake。

### partition_spec [string]

`partition_spec` Maxcompute分区表的此规范，例如：ds='20220101'。

### overwrite [boolean]

`overwrite` 是否覆盖表或分区，默认值：false。

### save_mode_create_template

我们使用模板自动创建MaxCompute表，
其将基于上游数据的类型和模式类型创建相应的表创建语句，
默认模板可以根据情况进行修改。目前仅适用于多表模式。

默认模板：

```sql
CREATE TABLE IF NOT EXISTS `${table}` (
${rowtype_fields}
) COMMENT '${comment}';
```

如果模板中填写了自定义字段，例如添加“id”字段

```sql
CREATE TABLE IF NOT EXISTS `${table}`
(   
    id,
    ${rowtype_fields}
) COMMENT '${comment}';
```

 连接器将自动从上游获得相应的类型以完成填充，
 并从`rowtype_fields`中删除id字段。此方法可用于自定义字段类型和属性的修改。

 您可以使用以下占位符

 -database：用于获取上游模式中的数据库
 -table_name：用于获取上游模式中的表名
 -rowtype_fields：用于获取上游模式中的所有字段，我们将自动映射到该字段
 MaxCompute的描述
 -rowtype_primary_key：用于获取上游模式中的主键（可能是列表）
 -rowtype_unique_key：用于获取上游模式中的唯一密钥（可能是列表）
 -comment：用于获取上游模式中的表注释

### schema_save_mode[Enum]

 在启动同步任务之前，对目标侧的现有表面结构选择不同的处理方案。
 选项介绍：  
 `RECREATE_SCHEMA`：当表不存在时将创建，保存表时将删除并重新生成。如果设置了“partition_spec”，则将删除并重建分区。
 `CREATE_SCHEMA_WHEN_NOT_EXIST`：当表不存在时将创建，保存表时跳过。如果设置了“partition_spec”，则将创建分区。
 `ERROR_WHEN_SCHEMA_NOT_EXIST`：当表不存在时，将报告错误
 `忽略：忽略表格的处理

### data_save_mode[Enum]

 在启动同步任务之前，对目标端的现有数据选择不同的处理方案。
 选项介绍：
 `DROP_DATA`：保留数据库结构并删除数据
 `APPEND_DATA`：保留数据库结构，保留数据
 `CUSTOM_PROCESSING `：用户定义的处理 
 `ERROR_WHEN_DE_EXISTS `：当有数据时，会报告错误

### custom_sql[String]

当data_save_mode选择CUSTOM_PROCESSING时，您应该填写CUSTOM_SQL参数。此参数通常填充可以执行的SQL。SQL将在同步任务之前执行。

### common 选项

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

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

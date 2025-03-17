# Maxcompute

> Maxcompute sink connector

## Description

Used to read data from Maxcompute.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|      name      |  type   | required | default value |
|----------------|---------|----------|---------------|
| accessId       | string  | yes      | -             |
| accesskey      | string  | yes      | -             |
| endpoint       | string  | yes      | -             |
| project        | string  | yes      | -             |
| table_name     | string  | yes      | -             |
| partition_spec | string  | no       | -             |
| overwrite      | boolean | no       | false         |
| common-options | string  | no       |               |

### accessId [string]

`accessId` Your Maxcompute accessId which cloud be access from Alibaba Cloud.

### accesskey [string]

`accesskey` Your Maxcompute accessKey which cloud be access from Alibaba Cloud.

### endpoint [string]

`endpoint` Your Maxcompute endpoint start with http.

### project [string]

`project` Your Maxcompute project which is created in Alibaba Cloud.

### table_name [string]

`table_name` Target Maxcompute table name eg: fake.

### partition_spec [string]

`partition_spec` This spec of Maxcompute partition table eg:ds='20220101'.

### overwrite [boolean]

`overwrite` Whether to overwrite the table or partition, default: false.

### save_mode_create_template

We use templates to automatically create MaxCompute tables,
which will create corresponding table creation statements based on the type of upstream data and schema type,
and the default template can be modified according to the situation. Only work on multi-table mode at now.

Default template:

```sql
CREATE TABLE IF NOT EXISTS `${table}` (
${rowtype_fields}
) COMMENT '${comment}';
```

If a custom field is filled in the template, such as adding an `id` field

```sql
CREATE TABLE IF NOT EXISTS `${table}`
(   
    id,
    ${rowtype_fields}
) COMMENT '${comment}';
```

The connector will automatically obtain the corresponding type from the upstream to complete the filling,
and remove the id field from `rowtype_fields`. This method can be used to customize the modification of field types and attributes.

You can use the following placeholders

- database: Used to get the database in the upstream schema
- table_name: Used to get the table name in the upstream schema
- rowtype_fields: Used to get all the fields in the upstream schema, we will automatically map to the field
  description of MaxCompute
- rowtype_primary_key: Used to get the primary key in the upstream schema (maybe a list)
- rowtype_unique_key: Used to get the unique key in the upstream schema (maybe a list)
- comment: Used to get the table comment in the upstream schema

### schema_save_mode[Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved. If the `partition_spec` is set, the partition will be deleted and rebuilt.        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved. If the `partition_spec` is set, the partition will be created.        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode[Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`CUSTOM_PROCESSING`：User defined processing  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

### custom_sql[String]

When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.

## Examples

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

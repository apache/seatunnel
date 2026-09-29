import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to write data to Maxcompute. The connector supports AccessKey (`accessId`/`accesskey`)
authentication, STS-token authentication, and the default Aliyun credentials provider chain. It can
append to or overwrite a target table or partition, create the target table from a template, and
uses an upload
or upsert session selected by `insert_strategy`.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| name                      | type    | required | default value                | description                                                                                                              |
|---------------------------|---------|----------|------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| accessId                  | string  | no       | -                            | Aliyun AccessKey ID used to access MaxCompute.                                                                            |
| accesskey                 | string  | no       | -                            | Aliyun AccessKey secret used to access MaxCompute.                                                                        |
| sts_token                 | string  | no       | -                            | STS token used for temporary MaxCompute authentication. When `sts_token` is provided, `accessId` and `accesskey` are required. |
| endpoint                  | string  | yes      | -                            | MaxCompute endpoint, starting with `http`.                                                                                |
| project                   | string  | yes      | -                            | MaxCompute project created in Alibaba Cloud.                                                                              |
| table_name                | string  | yes      | -                            | Target MaxCompute table name, for example `fake`.                                                                         |
| schema_name               | string  | no       | -                            | MaxCompute schema name (namespace between project and table). Required only when the table is in a non-default schema.    |
| partition_spec            | string  | no       | -                            | Partition spec for a MaxCompute partitioned table, for example `ds='20220101'`.                                           |
| overwrite                 | boolean | no       | false                        | Whether to overwrite the target table or partition.                                                                       |
| schema_save_mode          | enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | How to handle the target table before writing, such as `RECREATE_SCHEMA` or `CREATE_SCHEMA_WHEN_NOT_EXIST`.                |
| data_save_mode            | enum    | no       | APPEND_DATA                  | How to handle existing target data before writing, such as `DROP_DATA`, `APPEND_DATA`, or `ERROR_WHEN_DATA_EXISTS`.      |
| custom_sql                | string  | no       | -                            | Custom SQL to execute before writing when `data_save_mode = CUSTOM_PROCESSING`.                                          |
| save_mode_create_template | string  | no       | see below                    | DDL template used when the sink creates the target table.                                                                 |
| datetime_format           | string  | no       | yyyy-MM-dd HH:mm:ss          | Format string used to convert `LocalDateTime` fields to strings.                                                         |
| tunnel_endpoint           | string  | no       | -                            | Custom endpoint URL for the MaxCompute Tunnel service. When not set, the endpoint is auto-inferred from the region.       |
| tunnel_name               | string  | no       | -                            | Tunnel Quota name used for exclusive resource groups. Requires both `endpoint` and `tunnel_endpoint` to be VPC endpoints. |
| insert_strategy           | string  | no       | upload                       | Insert session strategy: `upload` uses an upload session, `upsert` uses an upsert session and requires a primary key.    |
| multi_table_sink_replica  | int     | no       | 1                            | Number of sink writer replicas for each table in a multi-table job.                                                      |
| common-options            |         | no       | -                            | Sink plugin common parameters, such as `plugin_input`.                                                                   |

### accessId [string]

`accessId` Your Maxcompute accessId that can access Alibaba Cloud.

### accesskey [string]

`accesskey` Your Maxcompute accessKey that can access Alibaba Cloud.

### sts_token [string]

`sts_token` Your MaxCompute STS Token for temporary authentication. **Note:** If `sts_token` is provided, `accessId` and `accesskey` are strictly required.

> **Passwordless Authentication (ECS RAM Role, Environment Variables, etc.)**
> To use passwordless authentication seamlessly, simply leave `accessId`, `accesskey`, and `sts_token` all blank. The connector will automatically fall back to the Aliyun DefaultCredentialsProvider chain (Environment Variables, System Properties, CLI Profiles, OIDC, ECS RAM Roles).

### endpoint [string]

`endpoint` Your Maxcompute endpoint start with http.

### project [string]

`project` Your Maxcompute project which is created in Alibaba Cloud.

### table_name [string]

`table_name` Target Maxcompute table name eg: fake.

### partition_spec [string]

`partition_spec` This spec of Maxcompute partition table eg:ds='20220101'.

### schema_name [string]

`schema_name` The MaxCompute Schema name (the namespace between Project and Table).
Only required when the table resides in a **non-default schema** within your MaxCompute project.
See [Schema-related operations](https://www.alibabacloud.com/help/en/maxcompute/user-guide/schema-related-operations).

Default: not set (uses the project default schema).

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

### schema_save_mode [Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved. If the `partition_spec` is set, the partition will be deleted and rebuilt.        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved. If the `partition_spec` is set, the partition will be created.        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode [Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`CUSTOM_PROCESSING`：User defined processing  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

### custom_sql [String]

When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.

### datetime_format [String]

User-defined format string used to convert LocalDateTime fields to strings.

Use this option when you want to specify a custom datetime format that matches one of the predefined values in DateTimeUtils.Formatter (e.g. yyyy-MM-dd HH:mm:ss, yyyyMMddHHmmss, etc.).

Example values:

- `yyyy-MM-dd HH:mm:ss`
- `yyyy-MM-dd HH:mm:ss.SSSSSS`
- `yyyy.MM.dd HH:mm:ss`
- `yyyy/MM/dd HH:mm:ss`
- `yyyy/M/d HH:mm`
- `yyyy-M-d HH:mm`
- `yyyy/M/d HH:mm:ss`
- `yyyy-M-d HH:mm:ss`
- `yyyyMMddHHmmss`

Default: `yyyy-MM-dd HH:mm:ss`

### tunnel_endpoint [String]

Specifies the custom endpoint URL for the MaxCompute Tunnel service.

By default, the endpoint is automatically inferred from the configured region.

This option allows you to override the default behavior and use a custom Tunnel endpoint.
If not specified, the connector will use the region-based default Tunnel endpoint.

In general, you do **not** need to set tunnel_endpoint. It is only needed for custom networking, debugging, or local development.

Example values:

- `https://dt.cn-hangzhou.maxcompute.aliyun.com`
- `https://dt.ap-southeast-1.maxcompute.aliyun.com`
- `http://maxcompute:8080`

Default: Not set (auto-inferred from region)

### tunnel_name [String]

`tunnel_name` Specifies the Tunnel Quota name for exclusive resource groups.

Tunnel Quota allows you to use dedicated computing resources for MaxCompute Tunnel data transfer, providing better performance and resource isolation.

**Important**: Tunnel Quota only works with **VPC (Virtual Private Cloud) endpoints**. It is not supported for public network access. You must configure both `endpoint` and `tunnel_endpoint` to use VPC endpoints when using `tunnel_name`.

If not specified, the default Tunnel quota will be used.

Example values:

- `your_tunnel_quota_name`

Default: Not set (use default quota)

### insert_strategy [string]

If `insert_strategy` is set to `upload`, insert operations use an upload session.
If set to `upsert`, insert operations use an upsert session. Upsert sessions require a primary key.

**Note**:
Using upload sessions for insert operations alongside update or delete operations may cause insert records to appear in the table later than expected.
When a primary key is present, it is recommended to set `insert_strategy` to `upsert` to ensure consistent upsert behavior.

`UPDATE_AFTER` and `DELETE` rows are always written through a MaxCompute upsert session, so the target table must have a primary key when the job contains update or delete rows. `UPDATE_BEFORE` rows are not supported by this sink.

### multi_table_sink_replica [int]

The number of writer replicas in multi-table sink mode. The default value is `1`.

Use this option when upstream data contains multiple table identifiers and `table_name` uses placeholders such as `${table_name}`. For example, `table_name = "${table_name}_sink"` writes upstream table `test_table` to target table `test_table_sink`.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Examples

### Append Data

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

### Multiple Tables

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

### Upsert or Delete Rows

Use `insert_strategy = "upsert"` when the upstream schema has a primary key and the job contains
update or delete rows. The example below uses an update row; delete rows use the same sink settings.

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

## Changelog

<ChangeLog />

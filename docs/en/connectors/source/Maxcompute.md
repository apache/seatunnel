import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from Maxcompute. The connector supports AccessKey (`accessId`/`accesskey`)
authentication, STS-token authentication, and the default Aliyun credentials provider chain
(environment variables, ECS RAM roles, and so on). It can read a single table with `table_name`,
a list of tables with
`table_list`, and supports partitioned tables, custom column lists, and a configurable `split_row`
for parallel reads.

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name            | type   | required                          | default value | description                                                                                                         |
|-----------------|--------|-----------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------|
| accessId        | string | no                                | -             | Aliyun AccessKey ID used to access MaxCompute.                                                                      |
| accesskey       | string | no                                | -             | Aliyun AccessKey secret used to access MaxCompute.                                                                  |
| sts_token       | string | no                                | -             | STS token used for temporary MaxCompute authentication. When `sts_token` is provided, `accessId` and `accesskey` are required. |
| endpoint        | string | yes                               | -             | MaxCompute endpoint, starting with `http`.                                                                          |
| project         | string | yes                               | -             | MaxCompute project created in Alibaba Cloud.                                                                        |
| table_name      | string | yes when `table_list` is not set  | -             | Target MaxCompute table name, for example `fake`.                                                                   |
| schema_name     | string | no                                | -             | MaxCompute schema name (namespace between project and table). Required only when the table is in a non-default schema. |
| partition_spec  | string | no                                | -             | Partition spec for a MaxCompute partitioned table, for example `ds='20220101'`.                                     |
| split_row       | int    | no                                | 10000         | Number of rows per split.                                                                                            |
| read_columns    | Array  | no                                | -             | Columns to read. When not set, all columns are read, for example `["col1", "col2"]`.                                |
| table_list      | Array  | no                                | -             | List of tables to read. Use this instead of `table_name` to read multiple MaxCompute tables in one job.              |
| tunnel_endpoint | string | no                                | -             | Custom endpoint URL for the MaxCompute Tunnel service. When not set, the endpoint is auto-inferred from the region. |
| tunnel_name     | string | no                                | -             | Tunnel Quota name used for exclusive resource groups. Requires both `endpoint` and `tunnel_endpoint` to be VPC endpoints. |
| schema          | config | no                                | -             | Schema of the source table. When `read_columns` is not set, all schema fields are read.                             |
| common-options  |        | no                                | -             | Source plugin common parameters, such as `plugin_output`.                                                           |

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

`table_name` Target Maxcompute table name, for example `fake`.

`table_name` and `table_list` are mutually exclusive. Use `table_name` for one table and `table_list` for multiple tables.

### partition_spec [string]

`partition_spec` This spec of Maxcompute partition table eg:ds='20220101'.

### schema_name [string]

`schema_name` The MaxCompute Schema name (the namespace between Project and Table).
Only required when the table resides in a **non-default schema** within your MaxCompute project.
See [Schema-related operations](https://www.alibabacloud.com/help/en/maxcompute/user-guide/schema-related-operations).

When using `table_list`, each entry can specify its own `schema_name`, which overrides the top-level value.

Default: not set (uses the project default schema).

### split_row [int]

`split_row` Number of rows per split, default: 10000.

### read_columns [Array]

`read_columns` The columns to be read, if not set, all columns will be read. e.g. ["col1", "col2"]

### table_list [Array]

The list of tables to be read, you can use this configuration instead of `table_name`.

Each table item must contain `table_name`. It can also override `project`, `schema_name`, `partition_spec`, `split_row`, and `read_columns`. If an item does not set those values, the connector uses the top-level value.

This mode is useful when one job needs to read several MaxCompute tables with the same account, endpoint, and default project.

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

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Examples

### Read with table

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

### Read with table list

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

## Changelog

<ChangeLog />

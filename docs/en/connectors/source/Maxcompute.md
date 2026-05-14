import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute source connector

## Description

Used to read data from Maxcompute.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name           | type   | required | default value |
|----------------|--------|----------|---------------|
| accessId       | string | no       | -             |
| accesskey      | string | no       | -             |
| sts_token      | string | no       | -             |
| endpoint       | string | yes      | -             |
| project        | string | yes      | -             |
| table_name     | string | yes      | -             |
| schema_name    | string | no       | -             |
| partition_spec | string | no       | -             |
| split_row      | int    | no       | 10000         |
| read_columns   | Array  | no       | -             |
| table_list     | Array  | No       | -             |
| common-options | string | no       |               |
| schema         | config | no       |               |

### accessId [string]

`accessId` Your Maxcompute accessId which cloud be access from Alibaba Cloud.

### accesskey [string]

`accesskey` Your Maxcompute accessKey which cloud be access from Alibaba Cloud.

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

When using `table_list`, each entry can specify its own `schema_name`, which overrides the top-level value.

Default: not set (uses the project default schema).

### split_row [int]

`split_row` Number of rows per split, default: 10000.

### read_columns [Array]

`read_columns` The columns to be read, if not set, all columns will be read. e.g. ["col1", "col2"]

### table_list [Array]

The list of tables to be read, you can use this configuration instead of `table_name`.

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

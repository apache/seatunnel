import ChangeLog from '../changelog/connector-hbase.md';

# Hbase

> Hbase Source Connector

## Description

Reads data from Apache Hbase.

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| Name                 | Type      | Required  | Default |
|----------------------|-----------|-----------|---------|
| zookeeper_quorum     | string    | Yes       | -       |
| table                | string    | Yes       | -       |
| schema               | config    | Yes       | -       |
| hbase_extra_config   | string    | No        | -       |
| caching              | int       | No        | -1      |
| batch                | int       | No        | -1      |
| cache_blocks         | boolean   | No        | false   |
| is_binary_rowkey     | boolean   | No        | false   |
| start_rowkey         | string    | No        | -       |
| end_rowkey           | string    | No        | -       |
| start_row_inclusive | boolean | No       | true    |
| end_row_inclusive   | boolean | No       | false   |
| common-options       |           | No        | -       |

### zookeeper_quorum [string]

The zookeeper quorum for Hbase cluster hosts, e.g., "hadoop001:2181,hadoop002:2181,hadoop003:2181".

### table [string]

The name of the table to write to, e.g., "seatunnel".

### schema [config]

Hbase stores data in byte arrays. Therefore, you need to configure the data types for each column in the table. For more information, see: [guide](../../concept/schema-feature.md#how-to-declare-type-supported).

### hbase_extra_config [config]

Additional configurations for Hbase.

### caching

The caching parameter sets the number of rows fetched per server trip during scans. This reduces round-trips between client and server, improving scan efficiency. Default: -1.

### batch

The batch parameter sets the maximum number of columns returned per scan. This is useful for rows with many columns to avoid fetching excessive data at once, thus saving memory and improving performance.

### cache_blocks

The cache_blocks parameter determines whether to cache data blocks during scans. By default, HBase caches data blocks during scans. Setting this to false reduces memory usage during scans. Default in SeaTunnel: false.

### is_binary_rowkey

The row key in HBase can be either a text string or binary data. In SeaTunnel, the row key is set to a text string by default (i.e., the default value of is_binary_rowkey is false).

### start_rowkey

The start row of the scan

### end_rowkey

The stop row of the scan

### start_row_inclusive

Whether to include the start row in the scan range. When set to true, the start row is included in the scan results. Default: true (inclusive).

**Note:** In most cases, you should keep the default value (true). Only modify this parameter if you have specific requirements for excluding the start row from your scan results.

### end_row_inclusive

Whether to include the end row in the scan range. When set to false, the end row is excluded from the scan results, following the left-closed-right-open convention [start, end). Default: false (exclusive).

**Note:** In most cases, you should keep the default value (false) which follows HBase's standard left-closed-right-open convention. Only modify this parameter if you need to include the end row in your scan results.

**Important:** When using parallel reading with multiple splits, the combination of these two parameters is critical for data integrity:
- **Default (start_row_inclusive=true, end_row_inclusive=false)**: This is the recommended configuration that ensures no data loss or duplication across splits. Each split follows the [start, end) convention.
- **Both false (start_row_inclusive=false, end_row_inclusive=false)**: This may cause **data loss** at split boundaries, as the boundary rows will be excluded from all splits.
- **Both true (start_row_inclusive=true, end_row_inclusive=true)**: This may cause **duplicate data** at split boundaries, as the boundary rows will be included in multiple adjacent splits.

### common-options

Common parameters for Source plugins, refer to [Common Source Options](../source-common-options.md).

## Example

```bash
source {
  Hbase {
    zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181" 
    table = "seatunnel_test" 
    caching = 1000 
    batch = 100 
    cache_blocks = false 
    is_binary_rowkey = false
    start_rowkey = "B"
    end_rowkey = "C"
    schema = {
      columns = [
        { 
          name = "rowkey" 
          type = string 
        },
        {
          name = "columnFamily1:column1"
          type = boolean
        },
        {
          name = "columnFamily1:column2" 
          type = double
        },
        {
          name = "columnFamily2:column1"
          type = bigint
        }
      ]
    }
  }
}
```

## Changelog

<ChangeLog />


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

|        Name        |  Type   | Required | Default |
|--------------------|---------|----------|---------|
| zookeeper_quorum   | string  | Yes      | -       |
| table              | string  | Yes      | -       |
| schema             | config  | Yes      | -       |
| hbase_extra_config | string  | No       | -       |
| caching            | int     | No       | -1      |
| batch              | int     | No       | -1      |
| cache_blocks       | boolean | No       | false   |
| common-options     |         | No       | -       |

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


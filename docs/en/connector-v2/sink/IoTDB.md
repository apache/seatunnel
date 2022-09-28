# IoTDB

> IoTDB sink connector

## Description

Used to write data to IoTDB.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

IoTDB supports the `exactly-once` feature through idempotent writing. If two pieces of data have
the same `key` and `timestamp`, the new data will overwrite the old one.

- [ ] [schema projection](../../concept/connector-v2-features.md)

:::tip

There is a conflict of thrift version between IoTDB and Spark.Therefore, you need to execute `rm -f $SPARK_HOME/jars/libthrift*` and `cp $IOTDB_HOME/lib/libthrift* $SPARK_HOME/jars/` to resolve it.

:::

## Options

| name                          | type              | required | default value |
|-------------------------------|-------------------|----------|---------------|
| node_urls                     | list              | yes      | -             |
| username                      | string            | yes      | -             |
| password                      | string            | yes      | -             |
| batch_size                    | int               | no       | 1024          |
| batch_interval_ms             | int               | no       | -             |
| max_retries                   | int               | no       | -             |
| retry_backoff_multiplier_ms   | int               | no       | -             |
| max_retry_backoff_ms          | int               | no       | -             |
| default_thrift_buffer_size    | int               | no       | -             |
| max_thrift_frame_size         | int               | no       | -             |
| zone_id                       | string            | no       | -             |
| enable_rpc_compression        | boolean           | no       | -             |
| connection_timeout_in_ms      | int               | no       | -             |
| timeseries_options            | list              | no       | -             |
| timeseries_options.path       | string            | no       | -             |
| timeseries_options.data_type  | string            | no       | -             |
| common-options                | string            | no       | -             |

### node_urls [list]

`IoTDB` cluster address, the format is `["host:port", ...]`

### username [string]

`IoTDB` user username

### password [string]

`IoTDB` user password

### batch_size [int]

For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the IoTDB

### batch_interval_ms [int]

For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the IoTDB

### max_retries [int]

The number of retries to flush failed

### retry_backoff_multiplier_ms [int]

Using as a multiplier for generating the next delay for backoff

### max_retry_backoff_ms [int]

The amount of time to wait before attempting to retry a request to `IoTDB`

### default_thrift_buffer_size [int]

Thrift init buffer size in `IoTDB` client

### max_thrift_frame_size [int]

Thrift max frame size in `IoTDB` client

### zone_id [string]

java.time.ZoneId in `IoTDB` client

### enable_rpc_compression [boolean]

Enable rpc compression in `IoTDB` client

### connection_timeout_in_ms [int]

The maximum time (in ms) to wait when connect `IoTDB`

### timeseries_options [list]

Timeseries options

### timeseries_options.path [string]

Timeseries path

### timeseries_options.data_type [string]

Timeseries data type

### common options [string]

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Examples

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    batch_size = 1024
    batch_interval_ms = 1000
  }
}
```
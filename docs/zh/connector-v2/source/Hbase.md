# Hbase

> Hbase 源连接器

## 描述

从 Apache Hbase 读取数据。

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [Schema](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../concept/connector-v2-features.md)

## 选项

|         名称         |   类型    | 必填 |  默认值  |
|--------------------|---------|----|-------|
| zookeeper_quorum   | string  | 是  | -     |
| table              | string  | 是  | -     |
| schema             | config  | 是  | -     |
| hbase_extra_config | string  | 否  | -     |
| caching            | int     | 否  | -1    |
| batch              | int     | 否  | -1    |
| cache_blocks       | boolean | 否  | false |
| common-options     |         | 否  | -     |

### zookeeper_quorum [string]

hbase的zookeeper集群主机，例如：“hadoop001:2181,hadoop002:2181,hadoop003:2181”

### table [string]

要写入的表名，例如：“seatunnel”

### schema [config]

Hbase 使用字节数组进行存储。因此，您需要为表中的每一列配置数据类型。有关更多信息，请参阅：[guide](../../concept/schema-feature.md#how-to-declare-type-supported)。

### hbase_extra_config [config]

hbase 的额外配置

### caching

caching 参数用于设置在扫描过程中一次从服务器端获取的行数。这可以减少客户端与服务器之间的往返次数，从而提高扫描效率。默认值:-1

### batch

batch 参数用于设置在扫描过程中每次返回的最大列数。这对于处理有很多列的行特别有用，可以避免一次性返回过多数据，从而节省内存并提高性能。

### cache_blocks

cache_blocks 参数用于设置在扫描过程中是否缓存数据块。默认情况下，HBase 会在扫描时将数据块缓存到块缓存中。如果设置为 false，则在扫描过程中不会缓存数据块，从而减少内存的使用。在SeaTunnel中默认值为: false

### 常用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../source-common-options.md)

## 示例

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


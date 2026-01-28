import ChangeLog from '../changelog/connector-hbase.md';

# Hbase

> Hbase 源连接器

## 描述

从 Apache Hbase 读取数据。

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [Schema](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                   | 类型       | 必填 | 默认值   |
|----------------------|----------|----|-------|
| zookeeper_quorum     | string   | 是  | -     |
| table                | string   | 是  | -     |
| schema               | config   | 是  | -     |
| hbase_extra_config   | config   | 否  | -     |
| caching              | int      | 否  | -1    |
| batch                | int      | 否  | -1    |
| cache_blocks         | boolean  | 否  | false |
| is_binary_rowkey     | boolean  | 否  | false |
| start_rowkey         | string   | 否  | -     |
| end_rowkey           | string   | 否  | -     |
| start_row_inclusive | boolean | 否  | true  |
| end_row_inclusive   | boolean | 否  | false |
| start_timestamp       | long    | 否  | -     |
| end_timestamp       | long    | 否  | -     |
| common-options       |          | 否  | -     |

### zookeeper_quorum [string]

hbase的zookeeper集群主机，例如：“hadoop001:2181,hadoop002:2181,hadoop003:2181”

### table [string]

要写入的表名，例如：“seatunnel”
如果表在自定义 namespace 下，请使用 `namespace:table` 形式（如 `ns1:seatunnel_test`）；未填写 namespace 时，SeaTunnel 会使用 HBase 的默认命名空间 `default`。

### schema [config]

Hbase 使用字节数组进行存储。因此，您需要为表中的每一列配置数据类型。有关更多信息，请参阅：[guide](../../introduction/concepts/schema-feature.md#how-to-declare-type-supported)。

### hbase_extra_config [config]

hbase 的额外配置

### caching

caching 参数用于设置在扫描过程中一次从服务器端获取的行数。这可以减少客户端与服务器之间的往返次数，从而提高扫描效率。默认值:-1

### batch

batch 参数用于设置在扫描过程中每次返回的最大列数。这对于处理有很多列的行特别有用，可以避免一次性返回过多数据，从而节省内存并提高性能。

### cache_blocks

cache_blocks 参数用于设置在扫描过程中是否缓存数据块。默认情况下，HBase 会在扫描时将数据块缓存到块缓存中。如果设置为 false，则在扫描过程中不会缓存数据块，从而减少内存的使用。在SeaTunnel中默认值为: false

### is_binary_rowkey

HBase 的行键既可以是文本字符串，也可以是二进制数据。在 SeaTunnel 中，行键默认设置为文本字符串(即 is_binary_rowkey 默认值为 false)

### start_rowkey

扫描起始行

### end_rowkey

扫描结束行

### start_row_inclusive

设置扫描范围是否包含起始行。当设置为 true 时,扫描结果将包含起始行。默认值: true (包含)。

**注意:** 在大多数情况下,应保持默认值 (true)。仅当您有特定需求需要排除起始行时才修改此参数。

### end_row_inclusive

设置扫描范围是否包含结束行。当设置为 false 时,扫描结果将不包含结束行,遵循左闭右开的区间约定 [start, end)。默认值: false (不包含)。

**注意:** 在大多数情况下,应保持默认值 (false),这遵循 HBase 标准的左闭右开区间约定。仅当您需要在扫描结果中包含结束行时才修改此参数。

**重要提示:** 在使用多个 split 并行读取时,这两个参数的组合对数据完整性至关重要:
- **默认配置 (start_row_inclusive=true, end_row_inclusive=false)**: 这是推荐的配置,可以确保跨 split 时不会丢失数据或产生重复数据。每个 split 遵循 [start, end) 左闭右开区间约定。
- **都设置为 false (start_row_inclusive=false, end_row_inclusive=false)**: 这可能会导致**数据丢失**,因为边界行会被所有 split 排除在外。
- **都设置为 true (start_row_inclusive=true, end_row_inclusive=true)**: 这可能会导致**数据重复**,因为边界行会被相邻的多个 split 重复包含。

### start_timestamp

时间范围扫描的起始时间戳(包含)。单位为毫秒(epoch)。时间范围遵循 [start, end) 左闭右开约定。如果只设置 start_timestamp，则最大值视为无限上界。

### end_timestamp

时间范围扫描的结束时间戳(不包含)。单位为毫秒(epoch)。时间范围遵循 [start, end) 左闭右开约定。如果只设置 end_timestamp，则最小值视为无限下界。

**说明:**

- `start_timestamp` / `end_timestamp` 必须大于等于 0；若两者同时配置，需要满足 `start_timestamp < end_timestamp`（遵循 [start, end) 约定，`start_timestamp == end_timestamp` 将导致空扫描）。
- 当 `start_rowkey` / `end_rowkey` 与 `start_timestamp` / `end_timestamp` 同时配置时，会同时应用行键范围与时间范围限制，最终返回两者的交集。

### 常用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../common-options/source-common-options.md)

## 示例

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
    start_timestamp = 1700000000000
    end_timestamp = 1700003600000
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

## Kerberos 示例

备注：

- `connector-hbase` 不会解析 `krb5_path` / `kerberos_principal` / `kerberos_keytab_path`。
- 需要在运行环境中提前完成 Kerberos 登录并保证 `krb5.conf` 可被 JVM 访问（例如 `kinit -kt ...` 或 JVM `-Djava.security.krb5.conf=...`），同时将 HBase/Hadoop 的安全配置写入 `hbase_extra_config`。

```hocon
source {
  Hbase {
    zookeeper_quorum = "zk1:2181,zk2:2181,zk3:2181"
    table = "source_table"
    caching = 1000
    batch = 200
    cache_blocks = false
    is_binary_rowkey = false

    # HBase安全配置
    hbase_extra_config = {
      "hbase.security.authentication" = "kerberos"
      "hadoop.security.authentication" = "kerberos"
      "hbase.master.kerberos.principal" = "hbase/_HOST@REALM"
      "hbase.regionserver.kerberos.principal" = "hbase/_HOST@REALM"
      "hbase.rpc.protection" = "authentication"
      "hbase.zookeeper.useSasl" = "false"
    }

    schema = {
      columns = [
        { name = "rowkey", type = string },
        { name = "info:name", type = string },
        { name = "info:score", type = string }
      ]
    }
  }
}
```

## 变更日志

<ChangeLog />

import ChangeLog from '../changelog/connector-hbase.md';

# Hbase

> Hbase 源连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

从 Apache HBase 表读取数据。支持普通扫描、行键范围扫描、时间戳范围扫描、二进制行键、自定义命名空间和并行批读取。

本连接器以批处理模式运行，读取的是扫描范围在任务启动时刻的快照状态，并非 CDC 源；扫描启动后发生的新写入不会进入结果。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [模式投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                 | 类型    | 必填 | 默认值 | 描述 |
|----------------------|---------|------|--------|------|
| zookeeper_quorum     | string  | 是   | -      | HBase ZooKeeper 地址，例如 `hadoop001:2181,hadoop002:2181`。 |
| table                | string  | 是   | -      | 要扫描的 HBase 表。自定义 namespace 请使用 `namespace:table`。 |
| schema               | config  | 是   | -      | SeaTunnel 表结构。行键写作 `rowkey`，普通单元格写作 `列簇:列名`。 |
| hbase_extra_config   | config  | 否   | -      | 额外的 HBase 或 Hadoop 客户端配置。 |
| caching              | int     | 否   | -1     | 每次 RPC 获取的行数。`-1` 表示使用 HBase 客户端默认值。 |
| batch                | int     | 否   | -1     | 每次 RPC 返回的最大单元格数量。`-1` 表示使用 HBase 客户端默认值。 |
| cache_blocks         | boolean | 否   | false  | 扫描结果是否写入 HBase block cache。 |
| is_binary_rowkey     | boolean | 否   | false  | 是否把行键字段按二进制字节处理。 |
| start_rowkey         | string  | 否   | -      | 范围扫描的起始行键。 |
| end_rowkey           | string  | 否   | -      | 范围扫描的结束行键。 |
| start_row_inclusive  | boolean | 否   | true   | 扫描结果是否包含 `start_rowkey`。 |
| end_row_inclusive    | boolean | 否   | false  | 扫描结果是否包含 `end_rowkey`。 |
| start_timestamp      | long    | 否   | -      | 时间范围扫描的起始时间戳，包含该时间。 |
| end_timestamp        | long    | 否   | -      | 时间范围扫描的结束时间戳，不包含该时间。 |
| common-options       |         | 否   | -      | Source 插件通用参数，例如 `plugin_output`。 |

### zookeeper_quorum [string]

HBase 的 zookeeper 集群主机，例如：“hadoop001:2181,hadoop002:2181,hadoop003:2181”

### table [string]

要读取的表名，例如：“seatunnel”
如果表在自定义 namespace 下，请使用 `namespace:table` 形式（如 `ns1:seatunnel_test`）；未填写 namespace 时，SeaTunnel 会使用 HBase 的默认命名空间 `default`。

### schema [config]

HBase 使用字节数组进行存储。因此，您需要为表中的每一列配置数据类型。
行键列使用 `rowkey`，HBase 单元格使用 `列簇:列名` 形式，例如 `info:name`。
更多信息请参阅：[模式声明指南](../../introduction/concepts/schema-feature.md#how-to-declare-type-supported)。

### hbase_extra_config [config]

HBase 的额外配置

### caching

caching 参数用于设置在扫描过程中一次从服务器端获取的行数。这可以减少客户端与服务器之间的往返次数，从而提高扫描效率。默认值:-1

### batch

batch 参数用于设置在扫描过程中每次返回的最大列数。这对于处理有很多列的行特别有用，可以避免一次性返回过多数据，从而节省内存并提高性能。

### cache_blocks

cache_blocks 参数用于设置在扫描过程中是否缓存数据块。默认情况下，HBase 会在扫描时将数据块缓存到块缓存中。如果设置为 false，则在扫描过程中不会缓存数据块，从而减少内存使用。在 SeaTunnel 中默认值为 false。

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

**重要提示:** 使用多个分片并行读取时，这两个参数的组合对数据完整性很重要:
- **默认配置 (start_row_inclusive=true, end_row_inclusive=false)**: 推荐配置，可以确保跨分片时不丢失数据、不产生重复数据。每个分片遵循 [start, end) 左闭右开区间约定。
- **都设置为 false (start_row_inclusive=false, end_row_inclusive=false)**: 可能导致**数据丢失**，因为边界行会被所有分片排除在外。
- **都设置为 true (start_row_inclusive=true, end_row_inclusive=true)**: 可能导致**数据重复**，因为边界行会被相邻的多个分片重复包含。

### start_timestamp

时间范围扫描的起始时间戳(包含)。单位为毫秒(epoch)。时间范围遵循 [start, end) 左闭右开约定。如果只设置 start_timestamp，则最大值视为无限上界。

### end_timestamp

时间范围扫描的结束时间戳(不包含)。单位为毫秒(epoch)。时间范围遵循 [start, end) 左闭右开约定。如果只设置 end_timestamp，则最小值视为无限下界。

**说明:**

- `start_timestamp` 必须大于等于 0，`end_timestamp` 必须大于 0；若两者同时配置，需要满足 `start_timestamp < end_timestamp`（遵循 [start, end) 约定，`start_timestamp == end_timestamp` 将导致空扫描）。
- 当 `start_rowkey` / `end_rowkey` 与 `start_timestamp` / `end_timestamp` 同时配置时，会同时应用行键范围与时间范围限制，最终返回两者的交集。

### 常用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../common-options/source-common-options.md)

## 示例

### 按行键和时间范围读取

```hocon
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

### 读取命名空间下的表

```hocon
source {
  Hbase {
    zookeeper_quorum = "hbase_e2e:2181"
    table = "ns1:seatunnel_test"
    schema = {
      columns = [
        { name = rowkey, type = string },
        { name = "info:name", type = string }
      ]
    }
  }
}
```

### 读取二进制行键

```hocon
source {
  Hbase {
    zookeeper_quorum = "hbase_e2e:2181"
    table = "binary_rowkey_table"
    is_binary_rowkey = true
    caching = 500
    batch = 100
    schema = {
      columns = [
        { name = rowkey, type = bytes },
        { name = "info:name", type = string },
        { name = "info:score", type = double }
      ]
    }
  }
}
```

当 `is_binary_rowkey = true` 时，请在 `schema` 中把行键列声明为 `bytes`，并在后续的 transform 节点里自行解码。

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

# StarRocks

> StarRocks 源连接器

## 描述

通过`StarRocks`读取外部数据源数据。
`StarRocks`源连接器的内部实现是从`FE`获取查询计划，
将查询计划作为参数传递给`BE`节点，然后从`BE`节点获取数据结果。

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户定义拆分](../../concept/connector-v2-features.md)

## 配置选项

| 名称                      | 类型     | 是否必须 | 默认值               |
|-------------------------|--------|------|-------------------|
| nodeUrls                | list   | 是    | -                 |
| username                | string | 是    | -                 |
| password                | string | 是    | -                 |
| database                | string | 是    | -                 |
| table                   | string | 是    | -                 |
| scan_filter             | string | 否    | -                 |
| schema                  | config | 是    | -                 |
| request_tablet_size     | int    | 否   | Integer.MAX_VALUE |
| scan_connect_timeout_ms | int    | 否   | 30000             |
| scan_query_timeout_sec  | int    | 否   | 3600              |
| scan_keep_alive_min     | int    | 否   | 10                |
| scan_batch_rows         | int    | 否   | 1024              |
| scan_mem_limit          | long   | 否   | 2147483648        |
| max_retries             | int    | 否   | 3                 |
| scan.params.*           | string | 否   | -                 |

### nodeUrls [list]

`StarRocks` 集群地址配置格式 `["fe_ip:fe_http_port", ...]`。

### username [string]

`StarRocks` 用户名称。

### password [string]

`StarRocks` 用户密码。

### database [string]

`StarRocks` 数据库名。

### table [string]

`StarRocks` 表名。

### scan_filter [string]

过滤查询的表达式，该表达式透明地传输到`StarRocks` 。`StarRocks` 使用此表达式完成源端数据过滤。

例如

```
"tinyint_1 = 100"
```

### schema [config]

#### fields [Config]

要生成的`starRocks`的`schema`

示例

```
schema {
    fields {
        name = string
        age = int
    }
  }
```

### request_tablet_size [int]

与分区对应的`StarRocks tablet`的数量。此值设置得越小，生成的分区就越多。这将增加引擎的平行度，但同时也会给`StarRocks`造成更大的压力。

以下示例，用于解释如何使用`request_tablet_size`来控制分区的生成。

```
StarRocks 集群中表的 tablet 分布作为 follower

be_node_1 tablet[1, 2, 3, 4, 5]
be_node_2 tablet[6, 7, 8, 9, 10]
be_node_3 tablet[11, 12, 13, 14, 15]

1.如果没有设置 request_tablet_size，则单个分区中的 tablet 数量将没有限制。分区将按以下方式生成：

partition[0] 从 be_node_1 读取 tablet 数据：tablet[1, 2, 3, 4, 5]
partition[1] 从 be_node_2 读取 tablet 数据：tablet[6, 7, 8, 9, 10]
partition[2] 从 be_node_3 读取 tablet 数据：tablet[11, 12, 13, 14, 15]

2.如果设置了 request_tablet_size=3，则每个分区中最多包含 3 个 tablet。分区将按以下方式生成

partition[0] 从 be_node_1 读取 tablet 数据：tablet[1, 2, 3]
partition[1] 从 be_node_1 读取 tablet 数据：tablet[4, 5]
partition[2] 从 be_node_2 读取 tablet 数据：tablet[6, 7, 8]
partition[3] 从 be_node_2 读取 tablet 数据：tablet[9, 10]
partition[4] 从 be_node_3 读取 tablet 数据：tablet[11, 12, 13]
partition[5] 从 be_node_3 读取 tablet 数据：tablet[14,15]
```

### scan_connect_timeout_ms [int]

发送到 `StarRocks` 的请求连接超时。

### scan_query_timeout_sec [int]

在 `StarRocks` 中，查询超时时间的默认值为 1 小时，-1 表示没有超时限制。

### scan_keep_alive_min [int]

查询任务的保持连接时长，单位是分钟，默认值为 10 分钟。我们建议将此参数设置为大于或等于 5 的值。
### scan_batch_rows [int]

一次从 `BE` 节点读取的最大数据行数。增加此值可以减少引擎与 `StarRocks` 之间建立的连接数量，从而减轻由网络延迟引起的开销。
### scan_mem_limit [long]

单个查询在 BE 节点上允许的最大内存空间，单位为字节，默认值为 2147483648 字节（即 2 GB）。

### max_retries [int]

发送到 `StarRocks` 的重试请求次数。

### scan.params. [string]

从 `BE` 节点扫描数据相关的参数。

## 示例

```
source {
  StarRocks {
    nodeUrls = ["starrocks_e2e:8030"]
    username = root
    password = ""
    database = "test"
    table = "e2e_table_source"
    scan_batch_rows = 10
    max_retries = 3
    schema {
        fields {
           BIGINT_COL = BIGINT
           LARGEINT_COL = STRING
           SMALLINT_COL = SMALLINT
           TINYINT_COL = TINYINT
           BOOLEAN_COL = BOOLEAN
           DECIMAL_COL = "DECIMAL(20, 1)"
           DOUBLE_COL = DOUBLE
           FLOAT_COL = FLOAT
           INT_COL = INT
           CHAR_COL = STRING
           VARCHAR_11_COL = STRING
           STRING_COL = STRING
           DATETIME_COL = TIMESTAMP
           DATE_COL = DATE
        }
    }
    scan.params.scanner_thread_pool_thread_num = "3"
    
  }
}
```

## Changelog

### next version

- Add StarRocks Source Connector


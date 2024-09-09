# Hbase

> Hbase 数据连接器

## 描述

将数据输出到hbase

## 主要特性

- [ ] [精准一次](../../concept/connector-v2-features.md)

## 选项

|         名称         |   类型    | 是否必须 |       默认值       |
|--------------------|---------|------|-----------------|
| zookeeper_quorum   | string  | yes  | -               |
| table              | string  | yes  | -               |
| rowkey_column      | list    | yes  | -               |
| family_name        | config  | yes  | -               |
| rowkey_delimiter   | string  | no   | ""              |
| version_column     | string  | no   | -               |
| null_mode          | string  | no   | skip            |
| wal_write          | boolean | yes  | false           |
| write_buffer_size  | string  | no   | 8 * 1024 * 1024 |
| encoding           | string  | no   | utf8            |
| hbase_extra_config | string  | no   | -               |
| common-options     |         | no   | -               |
| ttl                | long    | no   | -               |

### zookeeper_quorum [string]

hbase的zookeeper集群主机, 示例: "hadoop001:2181,hadoop002:2181,hadoop003:2181"

### table [string]

要写入的表名, 例如: "seatunnel"

### rowkey_column [list]

行键的列名列表, 例如: ["id", "uuid"]

### family_name [config]

字段的列簇名称映射。例如,上游的行如下所示：

| id |     name      | age |
|----|---------------|-----|
| 1  | tyrantlucifer | 27  |

id作为行键和其他写入不同列簇的字段，可以分配

family_name {
name = "info1"
age = "info2"
}

这主要是name写入列簇info1,age写入将写给列簇 info2

如果要将其他字段写入同一列簇，可以分配

family_name {
all_columns = "info"
}

这意味着所有字段都将写入该列簇 info

### rowkey_delimiter [string]

连接多行键的分隔符，默认 ""

### version_column [string]

版本列名称，您可以使用它来分配 hbase 记录的时间戳

### null_mode [double]

写入 null 值的模式，支持 [ skip , empty], 默认 skip

- skip: 当字段为 null ,连接器不会将此字段写入 hbase
- empty: 当字段为null时,连接器将写入并为此字段生成空值

### wal_write [boolean]

wal log 写入标志，默认值 false

### write_buffer_size [int]

hbase 客户端的写入缓冲区大小，默认 8 * 1024 * 1024

### encoding [string]

字符串字段的编码，支持[ utf8 ， gbk]，默认 utf8

### hbase_extra_config [config]

hbase扩展配置

### ttl [long]

hbase 写入数据 TTL 时间，默认以表设置的TTL为准，单位毫秒

### 常见选项

Sink 插件常用参数，详见 Sink 常用选项 [Sink Common Options](../sink-common-options.md)

## 案例

```hocon

Hbase {
  zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
  table = "seatunnel_test"
  rowkey_column = ["name"]
  family_name {
    all_columns = seatunnel
  }
}

```

### 写入多表

```hocon
env {
  # You can set engine configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
       {
        schema = {
          table = "hbase_sink_1"
         fields {
                    name = STRING
                    c_string = STRING
                    c_double = DOUBLE
                    c_bigint = BIGINT
                    c_float = FLOAT
                    c_int = INT
                    c_smallint = SMALLINT
                    c_boolean = BOOLEAN
                    time = BIGINT
           }
        }
            rows = [
              {
                kind = INSERT
                fields = ["label_1", "sink_1", 4.3, 200, 2.5, 2, 5, true, 1627529632356]
              }
              ]
       },
       {
       schema = {
         table = "hbase_sink_2"
              fields {
                    name = STRING
                    c_string = STRING
                    c_double = DOUBLE
                    c_bigint = BIGINT
                    c_float = FLOAT
                    c_int = INT
                    c_smallint = SMALLINT
                    c_boolean = BOOLEAN
                    time = BIGINT
              }
       }
           rows = [
             {
               kind = INSERT
               fields = ["label_2", "sink_2", 4.3, 200, 2.5, 2, 5, true, 1627529632357]
             }
             ]
      }
    ]
  }
}

sink {
  Hbase {
    zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
    table = "${table_name}"
    rowkey_column = ["name"]
    family_name {
      all_columns = info
    }
  }
}
```

## 写入指定列族

```hocon
Hbase {
  zookeeper_quorum = "hbase_e2e:2181"
  table = "assign_cf_table"
  rowkey_column = ["id"]
  family_name {
    c_double = "cf1"
    c_bigint = "cf2"
  }
}
```

## 更改日志

### 下一个版本

- 添加 hbase 输出连接器 ([4049](https://github.com/apache/seatunnel/pull/4049))


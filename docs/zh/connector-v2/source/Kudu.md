import ChangeLog from '../changelog/connector-kudu.md';

# Kudu

> Kudu 源连接器

## 支持 Kudu 版本

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义split](../../concept/connector-v2-features.md)

## 描述

用于从 Kudu 读取数据。

测试的 kudu 版本是 1.11.1。

## 数据类型映射

| Kudu 数据类型 | SeaTunnel 数据类型 |
|-------------|------------------|
| BOOL | BOOLEAN |
| INT8<br/>INT16<br/>INT32 | INT |
| INT64 | BIGINT |
| DECIMAL | DECIMAL |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| STRING | STRING |
| UNIXTIME_MICROS | TIMESTAMP |
| BINARY | BYTES |

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| kudu_masters | String | 是 | - | Kudu master 地址。用 ',' 分隔，例如 '192.168.88.110:7051'。 |
| table_name | String | 是 | - | Kudu 表的名称。 |
| client_worker_count | Int | 否 | 2 * Runtime.getRuntime().availableProcessors() | Kudu worker 数量。默认值是当前 CPU 核心数的两倍。 |
| client_default_operation_timeout_ms | Long | 否 | 30000 | Kudu 普通操作超时时间。 |
| client_default_admin_operation_timeout_ms | Long | 否 | 30000 | Kudu 管理操作超时时间。 |
| enable_kerberos | Bool | 否 | false | Kerberos principal 启用。 |
| kerberos_principal | String | 否 | - | Kerberos principal。注意所有 zeta 节点都需要有此文件。 |
| kerberos_keytab | String | 否 | - | Kerberos keytab。注意所有 zeta 节点都需要有此文件。 |
| kerberos_krb5conf | String | 否 | - | Kerberos krb5 conf。注意所有 zeta 节点都需要有此文件。 |
| scan_token_query_timeout | Long | 否 | 30000 | 连接扫描令牌的超时时间。如果未设置，将与 operationTimeout 相同。 |
| scan_token_batch_size_bytes | Int | 否 | 1024 * 1024 | Kudu 扫描字节数。一次读取的最大字节数，默认为 1MB。 |
| filter | String | 否 | - | Kudu 扫描过滤表达式，例如 id > 100 AND id < 200。 |
| schema | Map | 否 | 1024 * 1024 | SeaTunnel Schema。 |
| table_list | Array | 否 | - | 要读取的表列表。您可以使用此配置代替 `table_path`，例如：```table_list = [{ table_name = "kudu_source_table_1"},{ table_name = "kudu_source_table_2"}] ``` |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../source-common-options.md) 详见。 |

## 任务示例

### 简单

> 以下示例针对名为 "kudu_source_table" 的 Kudu 表，目标是在控制台打印此表中的数据并写入 kudu 表 "kudu_sink_table"

```hocon
# 定义运行时环境
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  kudu {
    kudu_masters = "kudu-master:7051"
    table_name = "kudu_source_table"
    plugin_output = "kudu"
    enable_kerberos = true
    kerberos_principal = "xx@xx.COM"
    kerberos_keytab = "xx.keytab"
  }
}

transform {
}

sink {
  console {
    plugin_input = "kudu"
  }

  kudu {
    plugin_input = "kudu"
    kudu_masters = "kudu-master:7051"
    table_name = "kudu_sink_table"
    enable_kerberos = true
    kerberos_principal = "xx@xx.COM"
    kerberos_keytab = "xx.keytab"
  }
}
```

### 多表

```hocon
env {
  # 您可以在此处设置引擎配置
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  kudu{
   kudu_masters = "kudu-master:7051"
   table_list = [
   {
    table_name = "kudu_source_table_1"
   },{
    table_name = "kudu_source_table_2"
   }
   ]
   plugin_output = "kudu"
}
}

transform {
}

sink {
  Assert {
    rules {
      table-names = ["kudu_source_table_1", "kudu_source_table_2"]
    }
  }
}
```

## 变更日志

<ChangeLog />


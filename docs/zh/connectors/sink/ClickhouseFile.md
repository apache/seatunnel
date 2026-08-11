import ChangeLog from '../changelog/connector-clickhouse.md';

# ClickhouseFile

> Clickhouse文件数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

该接收器使用clickhouse-local程序生成clickhouse数据文件，随后将其发送至clickhouse服务器，这个过程也称为bulkload。该接收器仅支持表引擎为 'Distributed'的表，且`internal_replication`选项需要设置为`true`。支持批和流两种模式。

## 主要特性

- [ ] [精准一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

:::tip 提示

你也可以采用JDBC的方式将数据写入Clickhouse。

:::

## 接收器选项

| 名称                     |   类型    | 是否必须 |                  默认值                   | 说明 |
|------------------------|---------|------|----------------------------------------|------|
| host                   | string  | yes  | -                                      | `ClickHouse` 集群地址，格式为 `host:port`，允许同时指定多个 `host`，例如 `"host1:8123,host2:8123"`。 |
| database               | string  | yes  | -                                      | `ClickHouse` 数据库名。 |
| table                  | string  | yes  | -                                      | 目标表名。表必须使用 `Distributed` 引擎并且 `internal_replication=true`。 |
| username               | string  | yes  | -                                      | 连接 `ClickHouse` 的用户名。 |
| password               | string  | yes  | -                                      | 连接 `ClickHouse` 的用户密码。 |
| clickhouse_local_path  | string  | yes  | -                                      | 每个 Worker 节点（Spark Executor、Flink TaskManager 或 Zeta worker）上 `clickhouse-local` 可执行文件的绝对路径。由于每个写入任务都会调用 `clickhouse-local`，所有运行 writer 的 Worker 必须提前在相同路径部署该可执行文件。 |
| sharding_key           | string  | no   | -                                      | 当需要拆分数据时，指定用于分片算法的字段；不填时由 writer 随机选择分片节点。 |
| copy_method            | string  | no   | scp                                    | 将暂存文件从 Worker 拷贝到 ClickHouse 节点所使用的方式，可选值：`scp`、`rsync`。 |
| node_free_password     | boolean | no   | false                                  | 当每个 Worker 到每个 ClickHouse 分片节点都可以免密登录（基于 SSH 密钥或 ssh-agent）时，设置为 `true`；否则需要通过 `node_pass` 配置每个节点的访问凭据。 |
| node_pass              | list    | no   | -                                      | 用于 `scp`/`rsync` 的逐节点凭据。仅在 `node_free_password=false` 且未配置 SSH 密钥时需要填写。 |
| node_pass.node_address | string  | no   | -                                      | ClickHouse 分片节点的地址。 |
| node_pass.username     | string  | no   | "root"                                 | ClickHouse 分片节点上的 SSH 用户名。 |
| node_pass.password     | string  | no   | -                                      | ClickHouse 分片节点上的 SSH 密码。当 `key_path` 配置且密钥认证成功时该字段被忽略。 |
| compatible_mode        | boolean | no   | false                                  | 当 ClickHouse 版本较旧、`clickhouse-local` 不支持 `--path` 参数时，设置为 `true`，连接器会改用不依赖 `--path` 的调用方式生成暂存文件。 |
| file_fields_delimiter  | string  | no   | "\t"                                   | 暂存 CSV 文件中的字段分隔符。值必须正好为一个字符，请选择业务字段中不会出现的字符。 |
| file_temp_path         | string  | no   | "/tmp/seatunnel/clickhouse-local/file" | Worker 本地用于保存暂存文件的目录；请保证磁盘空间足够容纳最大的批次写入量。 |
| key_path               | string  | no   | -                                      | `node_free_password=false` 时，`scp`/`rsync` 使用的 SSH 私钥文件绝对路径。配置后 `node_pass.password` 会被忽略，私钥必须已经写入每个分片节点的 `authorized_keys`。 |
| common-options         |         | no   | -                                      | Sink 插件通用参数，请参考 [Sink 常用选项](../common-options/sink-common-options.md) 获取更多细节信息。 |

### host [string]

`ClickHouse`集群地址，格式为`host:port`，允许同时指定多个`hosts`。例如`"host1:8123,host2:8123"`。

### database [string]

`ClickHouse`数据库名。

### table [string]

表名称。

### username [string]

连接`ClickHouse`的用户名。

### password [string]

连接`ClickHouse`的用户密码。

### sharding_key [string]

当ClickhouseFile需要拆分数据时，需要考虑的问题是当前数据需要发往哪个节点，默认情况下采用的是随机算法，我们也可以使用'sharding_key'参数为某字段指定对应的分片算法。

### clickhouse_local_path [string]

每个 Worker 节点（Spark Executor、Flink TaskManager 或 SeaTunnel Zeta worker）上 `clickhouse-local` 可执行文件的路径。
由于每个写入任务都会调用 `clickhouse-local`，所有运行 writer 的 Worker 必须提前在相同路径部署该可执行文件。
常见误区是只在 driver/master 节点部署可执行文件，而 writer 实际运行在 worker 上，第一个批次就会报
`clickhouse-local: command not found`。

### copy_method [string]

为文件传输指定方法，默认为scp，可选值为scp和rsync。

### node_free_password [boolean]

由于seatunnel需要使用scp或者rsync进行文件传输，因此seatunnel需要clickhouse服务端访问权限。如果每个spark节点与clickhouse服务端都配置了免密登录，则可以将此选项配置为true，否则需要在node_pass参数中配置对应节点的密码。

### node_pass [list]

用来保存所有clickhouse服务器地址及其对应的访问密码。

### node_pass.node_address [string]

clickhouse服务器节点地址。

### node_pass.username [string]

clickhouse服务器节点用户名，默认为root。

### node_pass.password [string]

clickhouse服务器节点的访问密码。

### compatible_mode [boolean]

在低版本的Clickhouse中，clickhouse-local程序不支持`--path`参数，需要设置该参数来采用其他方式实现`--path`参数功能。

### file_fields_delimiter [string]

ClickHouseFile使用CSV格式来临时保存数据。但如果数据中包含CSV的分隔符，可能会导致程序异常。使用此配置可以避免该情况。配置的值必须正好为一个字符的长度。

### file_temp_path [string]

ClickhouseFile本地存储临时文件的目录。

### key_path [string]

用于scp或rsync传输文件的私钥路径。

### common options

Sink插件常用参数，请参考[Sink常用选项](../common-options/sink-common-options.md)获取更多细节信息。

## 工作原理

ClickhouseFile 是一个 **bulk-load** 类型的接收器。每个 writer 的执行流程：

1. 将输入行缓存到本地 Worker 的 `file_temp_path` 下的 CSV 文件中。
2. 当缓冲区大小达到阈值或到达 checkpoint 屏障时，Worker 调用 `clickhouse_local_path` 处的 `clickhouse-local`
   将 CSV 转换为 ClickHouse 的本地存储格式。
3. Worker 通过 `scp` 或 `rsync`（免密登录、`node_pass` 凭据或 `key_path` 指向的 SSH 私钥）把转换后的文件拷贝到目标分片节点。
4. 分片节点通过 `Distributed` 表把文件加载进来。

由于最终的加载动作发生在 ClickHouse 一侧、由 `clickhouse-local` 完成，连接器本身并不参与分布式事务，因此
无法提供 Exactly-Once 语义。如果需要端到端的 Exactly-Once，请改用 JDBC 接收器，并在下游表上使用
`ReplacingMergeTree` 引擎配合主键去重策略。

## 示例

### 最小的 BATCH 作业

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  ClickhouseFile {
    host = "192.168.0.1:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
    clickhouse_local_path = "/opt/clickhouse/usr/bin/clickhouse-local"
    sharding_key = "age"
    node_free_password = false
    node_pass = [{
      node_address = "192.168.0.1"
      password = "seatunnel"
    }]
  }
}
```

### 多分片集群 + SSH 免密登录

集群有多个分片并且已经统一配置了 SSH 免密登录时，可以直接把 `node_free_password` 设置为 `true`，
不再填写 `node_pass`。连接器会根据 `sharding_key` 选分片，并复用现有的 SSH 配置完成拷贝。

```hocon
sink {
  ClickhouseFile {
    host = "shard-1:8123,shard-2:8123,shard-3:8123"
    database = "default"
    table = "orders_dist"
    username = "default"
    password = ""
    clickhouse_local_path = "/usr/local/bin/clickhouse-local"
    sharding_key = "id"
    node_free_password = true
    copy_method = "rsync"
    file_temp_path = "/data/seatunnel/clickhouse-tmp"
  }
}
```

### 基于 SSH 密钥的认证

当 `node_free_password=false` 但希望通过 SSH 密钥（而不是明文密码）认证时，把 `key_path` 指向私钥文件。
私钥必须已经写入每个分片节点的 `authorized_keys`。密钥认证成功时，`node_pass.password` 会被忽略。

```hocon
sink {
  ClickhouseFile {
    host = "shard-1:8123,shard-2:8123"
    database = "default"
    table = "events_dist"
    username = "default"
    password = ""
    clickhouse_local_path = "/usr/local/bin/clickhouse-local"
    sharding_key = "user_id"
    node_free_password = false
    node_pass = [{
      node_address = "shard-1"
      username = "clickhouse"
    }, {
      node_address = "shard-2"
      username = "clickhouse"
    }]
    key_path = "/etc/seatunnel/id_rsa"
    copy_method = "rsync"
  }
}
```

### 开启 Checkpoint 的 STREAMING 作业

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Kafka {
    # ...
  }
}

sink {
  ClickhouseFile {
    host = "shard-1:8123"
    database = "default"
    table = "events_dist"
    username = "default"
    password = ""
    clickhouse_local_path = "/usr/local/bin/clickhouse-local"
    node_free_password = true
    copy_method = "rsync"
  }
}
```

## 变更日志

<ChangeLog />

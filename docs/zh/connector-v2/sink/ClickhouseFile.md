# ClickhouseFile

> Clickhouse文件数据接收器

## 描述

该接收器使用clickhouse-local程序生成clickhouse数据文件，随后将其发送至clickhouse服务器，这个过程也称为bulkload。该接收器仅支持表引擎为 'Distributed'的表，且`internal_replication`选项需要设置为`true`。支持批和流两种模式。

## 主要特性

- [ ] [精准一次](../../concept/connector-v2-features.md)

:::tip 提示

你也可以采用JDBC的方式将数据写入Clickhouse。

:::

## 接收器选项

| 名称                     |   类型    | 是否必须 |                  默认值                   |
|------------------------|---------|------|----------------------------------------|
| host                   | string  | yes  | -                                      |
| database               | string  | yes  | -                                      |
| table                  | string  | yes  | -                                      |
| username               | string  | yes  | -                                      |
| password               | string  | yes  | -                                      |
| clickhouse_local_path  | string  | yes  | -                                      |
| sharding_key           | string  | no   | -                                      |
| copy_method            | string  | no   | scp                                    |
| node_free_password     | boolean | no   | false                                  |
| node_pass              | list    | no   | -                                      |
| node_pass.node_address | string  | no   | -                                      |
| node_pass.username     | string  | no   | "root"                                 |
| node_pass.password     | string  | no   | -                                      |
| compatible_mode        | boolean | no   | false                                  |
| file_fields_delimiter  | string  | no   | "\t"                                   |
| file_temp_path         | string  | no   | "/tmp/seatunnel/clickhouse-local/file" |
| key_path               | string  | no   | "/tmp/id_rsa"                          |
| common-options         |         | no   | -                                      |

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

在spark节点上的clickhouse-local程序路径。由于每个任务都会被调用，所以每个spark节点上的clickhouse-local程序路径必须相同。

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

Sink插件常用参数，请参考[Sink常用选项](../sink-common-options.md)获取更多细节信息。

## 示例

```hocon
ClickhouseFile {
  host = "192.168.0.1:8123"
  database = "default"
  table = "fake_all"
  username = "default"
  password = ""
  clickhouse_local_path = "/Users/seatunnel/Tool/clickhouse local"
  sharding_key = "age"
  node_free_password = false
  node_pass = [{
    node_address = "192.168.0.1"
    password = "seatunnel"
  }]
}
```

## 变更日志

### 2.2.0-beta 2022-09-26

- 支持将数据写入ClickHouse文件并迁移到ClickHouse数据目录

### 随后版本

- [BugFix] 修复生成的数据部分名称冲突BUG并改进文件提交逻辑  [3416](https://github.com/apache/seatunnel/pull/3416)
- [Feature] 支持compatible_mode来兼容低版本的Clickhouse  [3416](https://github.com/apache/seatunnel/pull/3416)


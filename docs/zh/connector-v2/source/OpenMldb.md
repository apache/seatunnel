# OpenMldb

> OpenMldb 源连接器

## 描述

用于从 OpenMldb 读取数据.

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 选项

|      名称           |  类型  | 必需 | 默认值 |
|-----------------|---------|----------|---------------|
| cluster_mode    | boolean | 是      | -             |
| sql             | string  | 是      | -             |
| database        | string  | 是      | -             |
| host            | string  | 否       | -             |
| port            | int     | 否       | -             |
| zk_path         | string  | 否       | -             |
| zk_host         | string  | 否       | -             |
| session_timeout | int     | 否       | 10000         |
| request_timeout | int     | 否       | 60000         |
| common-options  |         | 否       | -             |

### cluster_mode [string]

OpenMldb 是否处于群集模式

### sql [string]

Sql 语句

### database [string]

数据库名称

### host [string]

OpenMldb主机，仅支持OpenMldb单模

### port [int]

OpenMldb端口，仅支持OpenMldb单模

### zk_host [string]

Zookeeper主机，仅在OpenMldb集群模式下受支持

### zk_path [string]

Zookeeper路径，仅在OpenMldb集群模式下受支持

### session_timeout [int]

OpenMldb会话超时（ms），默认值60000

### request_timeout [int]

OpenMldb请求超时（ms），默认值为10000

### common options

源插件常用参数, 详见 [Source Common Options](../source-common-options.md) 

## 示例

```hocon

  OpenMldb {
    host = "172.17.0.2"
    port = 6527
    sql = "select * from demo_table1"
    database = "demo_db"
    cluster_mode = false
  }

```


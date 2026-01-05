import ChangeLog from '../changelog/connector-socket.md';

# Socket

> Socket 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

用于从 Socket 读取数据。

## 数据类型映射

文件没有特定的类型列表，我们可以通过在配置中指定 Schema 来指示相应的数据需要转换为哪种 SeaTunnel 数据类型。

| SeaTunnel 数据类型 |
|------------------|
| STRING |
| SHORT |
| INT |
| BIGINT |
| BOOLEAN |
| DOUBLE |
| DECIMAL |
| FLOAT |
| DATE |
| TIME |
| TIMESTAMP |
| BYTES |
| ARRAY |
| MAP |

## 选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| host | String | 是 | - | socket 服务器主机 |
| port | Integer | 是 | - | socket 服务器端口 |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。 |

## 如何创建 Socket 数据同步作业

* 配置 SeaTunnel 配置文件

以下示例演示如何创建从 Socket 读取数据并在本地客户端上打印的数据同步作业：

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# 创建源以连接到 socket
source {
    Socket {
        host = "localhost"
        port = 9999
    }
}

# 控制台打印读取的 socket 数据
sink {
  Console {
    parallelism = 1
  }
}
```

* 启动端口监听

```shell
nc -l 9999
```

* 启动 SeaTunnel 任务

* Socket 源发送测试数据

```text
~ nc -l 9999
test
hello
flink
spark
```

* 控制台 Sink 打印数据

```text
[test]
[hello]
[flink]
[spark]
```

## 变更日志

<ChangeLog />


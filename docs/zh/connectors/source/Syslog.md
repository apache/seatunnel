import ChangeLog from '../changelog/connector-syslog.md';

# Syslog

> Syslog 数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列裁剪](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 描述

通过 TCP 接收 syslog 消息，并按照 RFC 3164（BSD syslog）协议进行解析。该连接器作为服务端运行，
监听指定端口，接受来自 syslog 客户端（如 `rsyslog`、`syslog-ng`、硬件设备等）的连接。

该连接器支持多个 TCP 客户端连接，并会在已有客户端保持连接时继续监听新的客户端连接。

该连接器目前仅支持流处理模式。由于该连接器作为 TCP 监听服务运行，没有天然的输入结束点，
因此不支持批处理模式。

每条消息将被解析为结构化字段：设施码（facility）、严重级别（severity）、时间戳、主机名、应用名、
进程 ID 和消息内容。

## 配置项

| 名称           | 类型    | 必填 | 默认值    | 描述                                                                                          |
|----------------|---------|------|-----------|-----------------------------------------------------------------------------------------------|
| port           | Integer | 是   | -         | 监听传入 syslog 消息的 TCP 端口。                                                              |
| host           | String  | 否   | 0.0.0.0   | 绑定的网络接口地址。使用 `0.0.0.0` 接受所有网络接口上的连接。                                  |
| common-options |         | 否   | -         | 数据源插件公共参数，详情请参考 [Source 公共选项](../common-options/source-common-options.md)。  |

## 输出 Schema

每条 RFC 3164 syslog 消息将被解析为以下列：

| 列名      | 类型   | 描述                                                                     |
|-----------|--------|--------------------------------------------------------------------------|
| facility  | INT    | 设施码（0–23）。例如：0=内核，1=用户，4=认证，16=local0。                 |
| severity  | INT    | 严重级别（0–7）。0=紧急，3=错误，5=通知，6=信息，7=调试。                 |
| timestamp | STRING | 消息中的原始时间戳，例如 `Oct 11 22:14:15`。                              |
| hostname  | STRING | 来源设备的主机名或 IP 地址。                                               |
| app_name  | STRING | syslog TAG 字段中的应用或进程名称。                                        |
| proc_id   | STRING | syslog TAG 字段中的进程 ID，如不存在则为空字符串。                         |
| message   | STRING | 日志消息内容。                                                             |

## 示例

以下示例展示如何在 5140 端口接收 syslog 消息并将其输出到控制台：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Syslog {
    port = 5140
    host = "0.0.0.0"
  }
}

sink {
  Console {}
}
```

在 Linux 上可使用 `logger` 命令发送测试消息：

```bash
logger -n 127.0.0.1 -P 5140 -T "来自 syslog 连接器的测试消息"
```

## RFC 3164 消息格式

连接器期望接收以下格式的消息：

```
<优先级>时间戳 主机名 应用名[进程ID]: 消息内容
```

示例：

```
<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8
```

解析结果：

| facility | severity | timestamp       | hostname  | app_name | proc_id | message                                        |
|----------|----------|-----------------|-----------|----------|---------|------------------------------------------------|
| 4        | 2        | Oct 11 22:14:15 | mymachine | su       |         | 'su root' failed for lonvick on /dev/pts/8     |

<ChangeLog />
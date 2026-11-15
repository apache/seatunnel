import ChangeLog from '../changelog/connector-snmp.md';

# SNMP

> SNMPv2c 轮询 Source 连接器

## 描述

SNMP Source 通过 UDP 向 SNMP Agent 发送 SNMPv2c GET 请求，轮询显式配置的数字 OID 列表。每个返回的变量绑定会转换为一行数据。首个版本不支持 WALK、GETNEXT 或 GETBULK。

批处理作业只执行一次轮询，然后结束。流处理作业按照 `poll_interval_millis` 重复执行相同的 GET 请求。该 Source 使用单个 split，因此并行度必须为 1。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

该连接器使用 SNMP4J，支持通过 UDP 访问的 SNMPv2c Agent。

| 数据源 | 支持的版本 | 依赖 |
|--------|------------|------|
| SNMP Agent | SNMPv2c | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-snmp) |

## Source 参数

| 名称                 | 类型         | 必填 | 默认值 | 描述 |
|----------------------|--------------|------|--------|------|
| host                 | String       | 是   | -      | SNMP Agent 主机名或 IP 地址，不要包含协议或端口。 |
| port                 | Int          | 否   | 161    | SNMP Agent 的 UDP 端口。 |
| community            | String       | 是   | -      | SNMPv2c community 凭证。连接器不会把该值写入日志或错误信息。 |
| oids                 | List\<String\> | 是 | -      | GET 请求中的数字 OID 列表。允许前导点；不允许重复 OID 或符号 OID。 |
| timeout_millis       | Long         | 否   | 5000   | 每次请求尝试的超时时间，单位为毫秒。 |
| retries              | Int          | 否   | 1      | 首次请求失败后的重试次数。`0` 表示总共只发送一次请求。 |
| poll_interval_millis | Long         | 否   | 60000  | 两次流式轮询之间的延迟，单位为毫秒。批处理作业忽略此参数。 |

## 输出 Schema

输出 Schema 固定，不能通过 `schema` 参数修改。

| 字段       | 类型   | 描述 |
|------------|--------|------|
| agent      | string | 被轮询的 Agent，格式为 `host:port`。 |
| oid        | string | Agent 返回的数字 OID。 |
| value      | string | SNMP4J 对返回值的字符串表示。 |
| value_type | string | SMI 类型名称，例如 `Integer32`、`OctetString`、`TimeTicks` 或 `noSuchInstance`。 |
| poll_time  | long   | 轮询开始时的 Unix 纪元毫秒时间。同一次轮询的所有行使用相同值。 |

SNMP 值可能属于不同的 SMI 类型。V1 保留类型名称并输出 SNMP4J 的文本表示，不会把所有值强制转换为同一种数值类型。二进制值通过 `value` 字段传递时不保证可以逐字节还原。

## 批处理示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  SNMP {
    plugin_output = "snmp_metrics"
    host = "192.0.2.10"
    port = 161
    community = ${SNMP_COMMUNITY}
    oids = [
      "1.3.6.1.2.1.1.3.0",
      "1.3.6.1.2.1.1.5.0"
    ]
    timeout_millis = 3000
    retries = 1
  }
}

sink {
  Console {}
}
```

## 流处理示例

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  SNMP {
    plugin_output = "snmp_metrics"
    host = "192.0.2.10"
    community = ${SNMP_COMMUNITY}
    oids = ["1.3.6.1.2.1.1.3.0"]
    timeout_millis = 3000
    retries = 2
    poll_interval_millis = 30000
  }
}

sink {
  Console {}
}
```

## 失败与安全行为

- 用尽配置的所有请求尝试后仍超时，Source Task 会失败。
- SNMP 响应包含非零错误状态时，Source Task 会失败，并且不会输出部分响应行。
- 关闭或取消 Source 会关闭 SNMP transport，并停止后续轮询。
- `community` 是凭证。请通过配置替换或其他密钥管理方式提供，不要把真实值写入提交到版本控制的作业文件。

<ChangeLog />

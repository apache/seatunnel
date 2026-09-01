import ChangeLog from '../changelog/connector-snmp.md';

# SNMP

> SNMPv2c SET Sink 连接器

## 描述

SNMP Sink 为每一行输入向一个 SNMP Agent 发送一次同步 SNMPv2c SET 请求。
V1 范围仅包括 SET 操作，不发送 Trap 或 Inform，也不支持 SNMPv1 或 SNMPv3。

每一行需要提供数字 OID、字符串值和 SMI 值类型，对应的字段名可以配置。
默认映射会读取 SNMP Source 输出的 `oid`、`value` 和 `value_type` 字段；Sink 会忽略 `agent`、`poll_time` 等额外字段。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

连接器使用 SNMP4J，支持通过 UDP 访问的 SNMPv2c Agent。

| 数据源 | 支持版本 | 依赖 |
|--------|----------|------|
| SNMP Agent | SNMPv2c | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-snmp) |

## Sink 配置项

| 名称             | 类型   | 是否必填 | 默认值     | 描述 |
|------------------|--------|----------|------------|------|
| host             | String | 是       | -          | SNMP Agent 主机名或 IP 地址，不要包含协议或端口。 |
| port             | Int    | 否       | 161        | SNMP Agent 的 UDP 端口。 |
| community        | String | 是       | -          | SNMPv2c community 凭证。连接器不会把该值写入日志或错误信息。 |
| timeout_millis   | Long   | 否       | 5000       | 每次 SET 请求尝试的超时时间，单位为毫秒。 |
| retries          | Int    | 否       | 1          | 首次 SET 请求失败后的重试次数。`0` 表示只发送一次。 |
| oid_field        | String | 否       | oid        | 包含待设置数字 OID 的输入 `STRING` 字段。 |
| value_field      | String | 否       | value      | 包含待设置值的输入 `STRING` 字段。 |
| value_type_field | String | 否       | value_type | 包含 SMI 值类型的输入 `STRING` 字段。 |

三个映射字段必须存在于输入 Schema 中、类型必须为 `STRING`，并且不能指向同一个字段。Schema 错误会在创建任务时被拒绝。
空值以及空白的 OID 或值类型字段会在发送网络请求前被拒绝。值字段会根据其 SMI 类型进行校验；空的 `OctetString` 或 `OctetStringHex` 是有效值，文本 `OctetString` 的前后空白会被保留。

## 支持的 SMI 值类型

`value_type` 不区分大小写，并忽略空白、`_` 和 `-` 字符。
Sink 同时接受文档中的类型名和 SNMP Source 输出的 SNMP4J 语法字符串，包括 `Counter`、`Gauge`、`OCTET STRING` 和 `OBJECT IDENTIFIER`。

| 值类型 | 可接受的值 |
|--------|------------|
| `Integer32` 或 `Integer` | 有符号 32 位十进制整数。 |
| `UnsignedInteger32` 或 `UnsignedInteger` | 0 到 4294967295 的十进制整数。 |
| `Counter32` 或 `Counter` | 0 到 4294967295 的十进制整数。 |
| `Gauge32` 或 `Gauge` | 0 到 4294967295 的十进制整数。 |
| `TimeTicks` | 0 到 4294967295 的十进制百分之一秒计数，或 SNMP Source 使用的 SNMP4J 格式 `[days, ]hours:mm:ss.hh`。 |
| `Counter64` | 0 到 18446744073709551615 的十进制整数。 |
| `OctetString` 或 `OCTET STRING` | 输入字符串表示的 UTF-8 文本。 |
| `OctetStringHex` | 偶数个十六进制字符，例如 `00ff10`。 |
| `OID` 或 `OBJECT IDENTIFIER` | 数字对象标识符，可以带前导点。 |
| `IpAddress` | 点分 IPv4 地址。 |

`OctetString` 用于文本映射。如果需要逐字节保存二进制内容，请使用 `OctetStringHex`。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  shade.options = ["community"]
}

source {
  FakeSource {
    plugin_output = "snmp_updates"
    schema = {
      fields {
        oid = string
        value = string
        value_type = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = {
          oid = "1.3.6.1.2.1.1.5.0"
          value = "router-1"
          value_type = "OctetString"
        }
      }
    ]
  }
}

sink {
  SNMP {
    plugin_input = "snmp_updates"
    host = "192.0.2.10"
    port = 161
    community = ${SNMP_COMMUNITY}
    timeout_millis = 3000
    retries = 1
  }
}
```

`${SNMP_COMMUNITY}` 通过 SeaTunnel 的标准配置替换机制解析。请在已提交到源码的任务文件之外设置该值。
在 `shade.options` 中加入 `community`，还可以在记录解析后的任务配置时对该值进行脱敏。

## 投递、失败和安全语义

- 一次 `write` 成功表示 Agent 已对该行返回成功的 SNMP 响应。
- 所有配置尝试完成后仍超时，或 SNMP 响应包含非零错误状态时，Sink Task 会失败。
- 一行在失败前可能阻塞约 `timeout_millis * (retries + 1)`。请确保该时间小于任务的 Checkpoint 超时时间。
- SNMP4J 会重发超时请求。迟到的响应可能导致非幂等 OID 多次观察到同一次 SET。
- Sink 没有事务提交协议或可恢复的 Writer 状态。引擎恢复后可能重复发送 SET，因此投递语义为至少一次。
- 多个并行 Writer 可能乱序更新同一个 OID。如果更新顺序很重要，请使用并行度 1。
- Sink 不会把 RowKind 解释为 CDC 操作。所有输入行（包括更新或删除类型）都会作为 SET 请求处理。
- 请把 `community` 视为凭证，通过配置替换或其他密钥管理方式提供，不要把真实值提交到源码中的任务文件。
- SNMPv2c 不提供传输加密或完整性保护，community 和 SET 负载会以明文发送。请仅在可信私有网络中使用，或通过 VPN 等受保护隧道传输。
- Trap、Inform、SNMPv1 和 SNMPv3 不属于 V1 范围。

`plugin_input` 等配置请参阅[通用 Sink 配置项](../common-options/sink-common-options.md)。

<ChangeLog />

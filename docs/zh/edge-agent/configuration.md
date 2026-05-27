---
sidebar_position: 5
title: 配置参考
---

# Edge Agent 配置参数说明

Edge Agent 使用单一 YAML 配置文件（默认 config/agent.yaml）。本文是全量参数的类型、默认值与用户可见行为的权威说明。

示例文件：[seatunnel-edge-agent/config/agent.yaml](../../../seatunnel-edge-agent/config/agent.yaml)。建议先阅读 [快速开始](quick-start.md)；场景 YAML 见 [文件输入配置指南](input-configuration.md)、[输出配置指南](output-configuration.md)。

术语定义（WAL、BEST_EFFORT、WAL 行状态、Engine 响应码）见[术语表](about.md#术语表)。

## 顶层结构

```yaml
agent:    # 未配置时使用默认值
input:    # 必填 — 至少配置 paths（file 采集）
# queue:  # 未配置时 sqlite-path 默认 data/wal.db
# retry:  # 未配置时使用 retry 表默认值
output:   # 生产：type transport + endpoint（及 token）
```

:::note 配置结构

agent.yaml 仅使用顶层配置块：agent、input、queue、retry、output。省略 queue 时 sqlite-path 为 data/wal.db；省略 retry 时使用内置重试默认值。本地调试可用 output.type: console，其输出会以 EDGE_CONSOLE_OUTPUT 记录到 log/edge-agent.log，而非发送 EdgeSocket。

:::

## agent

进程级设置。


| 配置项                  | 类型     | 必填  | 默认值           | 说明                                                                                                                                                                             |
| -------------------- | ------ | --- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| id                 | string | 否   | 自动生成          | Agent 实例标识（日志、运维）。自动生成见 [身份文件](#身份文件edge-agentid)。                                                                                                                             |
| delivery-guarantee | string | 否   | BEST_EFFORT | 出站投递模式。本版本仅支持 BEST_EFFORT（别名：best-effort、best_effort）。未成功送达的数据会持久化并自动重试，同一记录在 output 侧可能出现多次，请为下游设计幂等消费。详见 [投递模式](./architecture-overview.md#62-投递模式)。 |
| idle-sleep-ms      | long    | 否   | 200         | 调度循环无进展时的休眠（毫秒）。必须 > 0。                           |
| bulk-max-size      | integer | 否   | 256         | 内存中待写入 WAL 的事件达到该条数时 flush。                        |
| flush-interval-ms  | long    | 否   | 1000        | 缓冲区非空后超过该毫秒数则 flush 到 WAL。 |


:::caution 关于 BEST_EFFORT

- 本地 WAL 持久化，并在 Engine 返回 RECEIVED 前自动重试（或超过 max-attempts 后标为 DEAD）。
- 发送失败、崩溃、resurrectSending 等可能导致同一 WAL 行多次发送。
- 需要严格去重时，请在 Sink 或业务层使用幂等键。

:::

## input

文件采集（type: file，默认）。每个 Agent 实例一个 input。

路径 glob、多行合并及分场景 YAML 见 [文件输入配置指南](input-configuration.md)。


| 配置项                     | 类型        | 必填    | 默认值      | 说明                                                            |
| ----------------------- | --------- | ----- | -------- | ------------------------------------------------------------- |
| id                    | string    | 否     | 自动生成     | 采集源标识；WAL/位点 sourceId。自动生成见 [身份文件](#身份文件edge-agentid)。      |
| type                  | string    | 否     | file   | 输入插件类型。当前仅实现 file。                                          |
| paths                 | string 列表 | 是 | —        | 采集文件 glob（如 /var/log/*.log）。不可为空，不可含空字符串。                   |
| encoding              | string    | 否     | UTF-8  | 文件编码。                                                         |
| read-from-beginning   | boolean   | 否     | false  | true：首次打开从文件头读。false：无已存位点时从 EOF 尾随。重启后仍按已保存字节 offset 恢复。 |
| glob-scan-interval-ms | long      | 否     | 5000   | 扫描 glob 发现新文件的间隔（毫秒）。                                         |
| close-inactive-ms     | long      | 否     | 300000 | 文件无读取活动超过该毫秒数后关闭句柄。                                           |
| on-error              | string    | 否     | skip   | 单文件 IO 错误：skip 跳过该文件继续；fail 终止 Agent。                     |


### input.multiline

多行日志合并。省略 multiline 或省略 pattern 时，一行物理行对应一个事件。


| 配置项         | 类型      | 必填  | 默认值     | 说明                                                             |
| ----------- | ------- | --- | ------- | -------------------------------------------------------------- |
| pattern   | string  | 否*  | —       | 事件边界正则。非空即启用多行模式。                                              |
| match     | string  | 否   | after | after：匹配行作为新事件起始，先 flush 缓冲区。before：匹配行作为当前事件最后一行。 |
| negate    | boolean | 否   | false | 是否对正则取反。                                                       |
| max-lines | integer | 否   | 500   | 单个多行事件最多合并的物理行数。                                               |
| flush-idle-timeout-ms | long | 否 | 5000 | 缓冲区空闲超过该毫秒数时强制 flush。启用多行时必须 > 0。 |


:::caution

启用多行模式时 pattern 为实际必填项。

:::

### input.output-format

写入 WAL / 发送前的事件序列化格式（不是 EdgeSocket 线协议格式）。


| 配置项    | 类型     | 必填  | 默认值    | 说明                                                                           |
| ------ | ------ | --- | ------ | ---------------------------------------------------------------------------- |
| type | string | 否   | line | line：每事件 JSON 包装（含 _file、_line、_offset、payload）。json：JSON 结构化输出。 |


### 嵌套 input.file

嵌套块用于在需要覆盖顶层同名字段时配置，字段与扁平 input 相同（paths、encoding、multiline、output-format 等）。同时存在时，file 块覆盖顶层同名字段。

## queue

SQLite WAL 出站缓冲及写入 WAL 前的内存批处理。


| 配置项                     | 类型      | 必填  | 默认值           | 说明                                                                      |
| ----------------------- | ------- | --- | ------------- | ----------------------------------------------------------------------- |
| sqlite-path           | string  | 否   | data/wal.db | SQLite 数据库文件路径（WAL + 位点）。父目录 data/ 会自动创建。相对路径相对进程工作目录（脚本启动时一般为安装根目录）。 |
| poll-batch-size       | integer | 否   | 128         | 每轮调度从 WAL claim 的最大行数；同时限制该轮输入轮询批量。                                     |
| cleanup-batch-size    | integer | 否   | 128         | 每次清理最多删除的 ACKED 行数。                                                     |
| acked-retention-ms    | long    | 否   | 0           | ACKED 行保留时长（毫秒）；0 表示清理时尽快删除（受 batch 限制）。                              |
| resurrect-batch-size  | integer | 否   | 100         | 每次将 SENDING 恢复为 PENDING 的最大行数（崩溃恢复）。                                |
| resurrect-interval-ms | long    | 否   | 60000       | 恢复扫描间隔（毫秒），同时作为 SENDING 行的过期阈值。必须 > 0。 |


### SQLite 持久化文件

:::note SQLite 文件说明

sqlite-path 指向单个数据库文件路径（不是目录）。默认 data/wal.db 位于安装根 data/ 下，WAL 模式下同目录还有 -wal、-shm 伴生文件。

:::

| 项    | 说明                                                                                 |
| ---- | ---------------------------------------------------------------------------------- |
| 路径   | 默认 data/wal.db；可改为其他相对/绝对路径                                    |
| 磁盘文件 | 主库 + -wal、-shm 伴生文件                                                            |
| 库内数据 | 出站队列 edge_agent_wal；采集位点 edge_agent_source_position（source_id = input.id） |
| 迁移   | 与 edge-agent.id 一并拷贝；多实例勿共用同一 sqlite-path                                      |

更多问答见 [常见问题](faq.md)。

## retry

WAL 行发送重试策略。YAML 中未配置 retry 时，采用下表默认值。


| 配置项              | 类型      | 必填  | 默认值      | 说明                                                |
| ---------------- | ------- | --- | -------- | ------------------------------------------------- |
| max-attempts   | integer | 否   | 16     | 单行最大发送尝试次数（attempt_count）；达到上限后标记为 DEAD，不再发送。 |
| backoff-ms     | long    | 否   | 250    | 调度器在 WAL 批次发送失败后的基础退避（毫秒），与 transport 重连退避不同。     |
| backoff-max-ms | long    | 否   | 300000 | 调度器发送退避上限（毫秒），须 ≥ backoff-ms。                   |


## output

出站目标。省略 type 时默认为 console。

endpoint/token 对齐、RAW 与 PACKET 及场景 YAML 见 [输出配置指南](output-configuration.md)。


| 配置项    | 类型     | 必填  | 默认值       | 说明                                                                                                    |
| ------ | ------ | --- | --------- | ----------------------------------------------------------------------------------------------------- |
| id   | string | 否   | 自动生成      | 出站逻辑标识（日志、迁移；当前不写入线协议）。自动生成见 [身份文件](#身份文件edge-agentid)。                                               |
| type | string | 否   | console | transport：EdgeSocket 客户端。console：将 payload 以 EDGE_CONSOLE_OUTPUT 写入 log/edge-agent.log（调试用途）。 |


### type: transport 时的 output


| 配置项                       | 类型      | 必填    | 默认值     | 说明                                                                                                  |
| ------------------------- | ------- | ----- | ------- | --------------------------------------------------------------------------------------------------- |
| endpoint                | string  | 是 | —       | Collector 地址 host:port，须与 Engine 作业 [EdgeSocket Source](../connectors/source/EdgeSocket.md) 监听一致。 |
| auth-type               | string  | 否     | token | 不支持 auth-type 为 none；须与 Engine EdgeSocket Source 鉴权配置一致。                                          |
| token                   | string  | 是 | —       | `__AUTH__` 共享密钥；须与 Engine 作业中 EdgeSocket token 一致。                                                |
| connect-timeout-ms      | integer | 否     | 5000  | TCP 连接超时（毫秒）。                                                                                       |
| read-timeout-ms         | integer | 否     | 30000 | TCP 读超时（毫秒）。                                                                                        |
| max-batch-send-attempts | integer | 否     | 64    | 单批次发送失败前的尝试次数，之后触发重连逻辑。                                                                             |
| initial-backoff-ms      | long    | 否     | 100   | 重连初始退避（毫秒）。                                                                                         |
| max-backoff-ms          | long    | 否     | 30000 | 重连最大退避（毫秒）。                                                                                         |
| max-reconnect-cycles    | integer | 否     | 16    | 失败批次的最大重连轮数。                                                                                        |
| packet-mode             | string  | 否     | RAW   | RAW：按行发送。PACKET：分帧，并支持压缩/加密。                                                                     |
| compression             | string  | 否     | gzip  | 仅 PACKET 模式（packet-mode 为 RAW 时忽略）：none、gzip、zlib、deflate。                          |
| encryption              | string  | 否     | none  | PACKET 模式：none 或 aes_gcm。                                                                   |
| aes-secret-key-base64   | string  | 条件    | —       | Base64 AES 密钥；encryption=aes_gcm 时必填。                                                             |


### type: console 时的 output

无额外配置项，payload 以 EDGE_CONSOLE_OUTPUT 写入 log/edge-agent.log。

## 身份文件

YAML 中省略 agent.id、input.id、output.id 时，Agent 在安装根目录读写 安装根/edge-agent.id（与 edge-agent.pid 同级）。YAML 显式 id 优先。


| 键 / YAML    | 作用                                            |
| ----------- | --------------------------------------------- |
| agent.id  | Agent 实例标识（日志、同一主机多实例区分）                      |
| input.id  | 采集源标识；作为 WAL / 位点的 sourceId，重启后续读、避免位点按新源处理 |
| output.id | 出站逻辑标识（迁移与日志；当前版本不写入 EdgeSocket 线协议）          |


示例：

```text
agent.id=<uuid>
input.id=<uuid>
output.id=<uuid>
```

:::caution

迁移或升级时请保留 edge-agent.id 与 SQLite 库文件（默认 data/wal.db 及伴生文件）。若丢失且 YAML 未配置 input.id，会生成新 ID，已有位点不再适用。

:::

更多问答见 [常见问题](faq.md)。

## 启动脚本环境变量

:::note

EDGE_AGENT_CONFIG、EDGE_AGENT_PID_FILE、EDGE_AGENT_ID_FILE 等启动脚本变量见 [运维 — 环境变量](operations.md#环境变量)，不属于 agent.yaml。

:::

## 相关文档

- [快速开始](quick-start.md)
- [部署指南](deployment-guide.md)
- [文件输入配置指南](input-configuration.md)
- [输出配置指南](output-configuration.md)
- [架构概览](./architecture-overview.md)
- [EdgeSocket Source](../connectors/source/EdgeSocket.md)


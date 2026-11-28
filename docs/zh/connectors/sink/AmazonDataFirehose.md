import ChangeLog from '../changelog/connector-amazondatafirehose.md';

# Amazon Data Firehose

{/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/}

通过 `PutRecordBatch` 写入已有的 Amazon Data Firehose DirectPut 投递流。连接器对应的插件是 `connector-amazondatafirehose`。为目标投递流授予 `firehose:PutRecordBatch` 权限。Firehose 负责向配置的下游目的地投递数据。[AWS Firehose 批量写入 API](https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html)

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)，Zeta 需设置 `sink.flush.interval`

## Sink 选项

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `delivery_stream` | String | 是 | | 已有 DirectPut 投递流的名称，长度为 1–64 个字符，可用英文字母、数字、点、下划线和连字符。 |
| `region` | String | 是 | | 用于解析 Firehose 端点和签名的 AWS 区域。 |
| `access_key` | String | 否 | | 显式配置的 AWS 访问密钥 ID。与 `secret_key` 同时提供，或均省略以使用工作节点环境变量。 |
| `secret_key` | String | 否 | | 显式配置的 AWS 私密访问密钥。 |
| `token` | String | 否 | | 可选的 AWS 会话令牌，与显式密钥对一起使用。 |
| `endpoint_url` | String | 否 | | 可信 HTTPS 端点。可带端口和末尾斜杠。 |
| `allow_insecure_http` | Boolean | 否 | false | 仅允许显式配置、可信且隔离的开发端点使用明文 HTTP。 |
| `format` | Enum | 否 | JSON | `JSON`、`DELIMITED`、`JSON_LINES` 或 `RAW`。 |
| `field_delimiter` | String | 否 | `,` | `DELIMITED` 的单字符字段分隔符，不能使用引号或换行符。 |
| `max_record_bytes` | Integer | 否 | 1024000 | Base64 编码前单条记录的最大字节数，含行分隔符，取值 1–1024000。 |
| `batch_size` | Integer | 否 | 100 | 每批最大记录数，取值 1–500。 |
| `batch_timeout_ms` | Integer | 否 | 30000 | 每次写入批次的总时限，包含所有尝试和退避，取值 1–300000 毫秒。 |
| `max_retries` | Integer | 否 | 2 | 对明确失败记录的最大重试次数，取值 0–10。 |
| common-options | | 否 | | [Sink 通用选项](../common-options/sink-common-options.md)。 |

原生 `JSON` 和 `DELIMITED` 同时使用该上限限制输出分配前的展开转换工作量。重复值、容器、字段名和每个 map 条目都分别计入；转换成同一个 JSON key 的条目也不会去重。因此，即使 key 覆盖会缩小最终 JSON，输入仍可能超出工作量上限而被拒绝。JSON 保留原生标量格式与 key 覆盖顺序，不新增固定嵌套深度限制。DELIMITED 保留原生格式器及其既有分隔符层级限制。非法值或超限输入在调用服务前失败，错误信息不包含数据值。

未显式填写密钥对时，工作节点读取 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` 和可选的 `AWS_SESSION_TOKEN`。请通过部署环境的凭证管理机制向工作节点提供密钥。连接器当前不直接读取 AWS 配置档案或调用实例元数据凭证接口。

工作节点必须能够直接连接端点。当前传输不会读取 JVM/SOCKS 或 SDK HTTP 代理配置；代理和 CONNECT 路由不属于本连接器的初始能力范围。

## 输入格式

输入记录必须为 `INSERT`。每条 SeaTunnel 记录对应一条 Firehose 记录。

| 格式 | 输入 | 输出 |
| --- | --- | --- |
| `JSON` | 结构化 SeaTunnel 记录 | UTF-8 JSON 对象，末尾添加 LF。 |
| `DELIMITED` | 结构化 SeaTunnel 记录 | 按 CSV 引号规则生成 UTF-8 分隔记录，末尾添加 LF。逗号对应 CSV，制表符对应 TSV。空值输出为空字段。 |
| `JSON_LINES` | 单个名为 `payload` 的 STRING 字段 | 校验单行 JSON 对象后追加 LF，保留原始 JSON 表示和小数精度。 |
| `RAW` | 单个名为 `payload` 的 STRING 或 BYTES 字段 | 字符串编码成 UTF-8，字节原样写入。按下游读取要求在输入中提供分隔符。 |

记录数达到阈值或预计请求将超过 4 MiB 时，连接器切分批次，并校验实际请求大小。检查点和最终提交准备阶段会发送剩余记录。Zeta 设置正数 `env.sink.flush.interval` 后，也可以在达到批量阈值前定时发送。低流量流式任务可配置检查点或刷新周期。[AWS Firehose 配额](https://docs.aws.amazon.com/firehose/latest/dev/limits.html)

## 投递与重试

确认响应通过校验后，表示 Firehose 已接收数据并将异步投递。连接器只重试完整且一致的批次响应中明确失败的位置。整批调用错误、响应无效或连接丢失会导致作业失败。连接器不会自动重发整批，因为 Firehose 可能已接收其中一些记录。

检查点恢复可能重发已接收的记录，SeaTunnel 无法回滚 Firehose 已接收的数据。需要去重时，由下游处理。可使用 Firehose 投递日志和指标核实数据是否到达最终目的地。批量写入和部分重试不保证下游顺序。

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  FakeSource {
    schema = {
      fields {
        event_id = int
        detail = string
      }
    }
    rows = [{ kind = INSERT, fields = [1, "库存变动"] }]
  }
}

sink {
  AmazonDataFirehose {
    delivery_stream = "inventory-events"
    region = "us-east-1"
    format = JSON
  }
}
```

## 更新日志

<ChangeLog />

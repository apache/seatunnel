import ChangeLog from '../changelog/connector-http-solr.md';

# Solr

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

通过 HTTP 将文档写入已有的 Apache Solr Core 或 SolrCloud 集合。插件为 `connector-http-solr`。连接账号需有目标 Core 的 Schema API 唯一键读取权限和 `/update` 写入权限。[Solr 更新 API](https://solr.apache.org/guide/solr/latest/indexing-guide/indexing-with-update-handlers.html)

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [完整 CDC 支持](../../introduction/concepts/connector-v2-features.md)
- [ ] [多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)，Zeta 需启用 `sink.flush.interval`

## Sink 选项

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `base_url` | String | 是 | | HTTPS 服务地址，例如 `https://solr.example:8983`，可带末尾斜杠。不得包含 Core 路径、查询参数、片段或凭证。 |
| `collection` | String | 是 | | 已创建的目标 Solr Core 或集合名称。连接器会拼接 `/solr/<collection>`。 |
| `username` | String | 否 | | 可选的 Basic 认证用户名，和 `password` 同时设置。 |
| `password` | String | 否 | | 可选的 Basic 认证密码。共享作业建议使用密钥或变量替换。 |
| `allow_insecure_http` | Boolean | 否 | false | 仅对显式信任的隔离端点允许明文 HTTP。 |
| `unique_key` | String | 否 | id | 目标文档 ID 字段。须与 Core 的唯一键一致，并映射到 STRING 或整数输入列。 |
| `field_mapping` | Map | 否 | `{}` | 输入列名到目标字段名的映射，未配置映射的列使用原名称。 |
| `enable_delete` | Boolean | 否 | false | 允许 `DELETE` 记录，仅按提供的文档 ID 删除。 |
| `batch_size` | Integer | 否 | 100 | 同一 HTTP 请求内连续且同类操作的最大行数，范围 1–500。设置为 1 时逐条发送。 |
| `commit_mode` | Enum | 否 | HARD | `HARD`、`SOFT` 或 `NONE`，在检查点、最终提交准备或 Zeta 定时刷新边界生效。 |
| `request_timeout_ms` | Integer | 否 | 30000 | 单次 HTTP 请求的总时限，范围 1–300000 毫秒。 |
| common-options | | 否 | | [Sink 通用选项](../common-options/sink-common-options.md)。 |

工作节点必须能够直接连接所配置的服务地址。传输层不会读取 JVM/SOCKS 或标准 HTTP 客户端代理配置；代理和 CONNECT 路由不属于本连接器的能力范围。

## 输入与更新语义

`INSERT` 和 `UPDATE_AFTER` 将完整文档写入 Solr，同一唯一键的旧文档会被替换。未设置或为 null 的字段不写入，替换时这些字段的旧值随旧文档一起移除。`UPDATE_BEFORE` 会校验失败。启用 `DELETE` 后，连接器只读取唯一键值，并按 ID 删除指定文档。

字符串、布尔值、整数、有限浮点数和受支持的标量数组可映射到普通 Solr 字段。数组须写入多值字段，数组中的 null 元素被忽略。DECIMAL 以十进制文本传输。需要保留每一位小数时，请使用 Solr 字符串字段。日期和无时区时间戳按 UTC 转换。带时区时间戳使用实际时刻，Solr 日期字段的存储精度由目标 Schema 决定。TIME 以字符串传输，BYTES 以 Base64 文本传输。Solr 负责校验必填字段和实际字段类型。此输入契约不支持嵌套文档、任意 Map、原子更新操作符或按查询删除。

首次写入前校验目标唯一键。文档上限为 1 MiB，请求上限为 4 MiB。单个写入器保持索引和删除操作的输入顺序。多条记录操作同一文档且要求顺序时，请使用单个写入器。

## 提交与恢复

`HARD` 发出硬提交并等待搜索器可用。`SOFT` 等待搜索器刷新，不请求硬提交。`NONE` 由 Solr 控制提交和搜索可见时机。提交可能同时使 Core 上其他待提交更新可见。空刷新不会发起请求。Zeta 稀疏流可设置 `env.sink.flush.interval` 定期刷新。[Solr 提交和事务日志](https://solr.apache.org/guide/solr/latest/configuration-guide/commits-transaction-logs.html)

批次和提交均要求 HTTP 200、Solr 成功状态且没有部分错误。拒绝或结果不确定时作业失败。错误发生前 Solr 可能已执行部分操作。连接器不会自动重试或回滚整个 Core。重放失败作业前请先核对 Solr 中的数据。引擎恢复可能重新发送已执行的记录。Solr 写入不参与 SeaTunnel 的检查点事务，数据可能早于检查点可见。

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
  job.retry.times = 0
}

source {
  FakeSource {
    schema = { fields { event_key = string, title = string } }
    rows = [{ kind = INSERT, fields = ["evt-1", "库存更新"] }]
  }
}

sink {
  Solr {
    base_url = "https://solr.example:8983"
    collection = "events"
    field_mapping = { event_key = "id", title = "title_s" }
    commit_mode = HARD
  }
}
```

## 更新日志

<ChangeLog />

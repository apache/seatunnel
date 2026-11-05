import ChangeLog from '../changelog/connector-http-amplitude.md';

# Amplitude

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

## 描述

通过 [Amplitude Export API](https://amplitude.com/docs/apis/analytics/export) 读取原始事件。
此连接器仅支持有界批处理，不支持写入、持续订阅或 CDC。复用现有 HTTP 和 JSON 依赖，不新增 SDK。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] Exactly-once
- [ ] 列投影
- [ ] 并行读取

请设置 `parallelism = 1` 和 `job.mode = BATCH`。

## 配置

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| api_key | String | 是 | - | Amplitude 项目 API 密钥，作为 Basic 认证的用户名。 |
| secret_key | String | 是 | - | 项目私密密钥，作为 Basic 认证的密码。 |
| start | String | 是 | - | 包含在内的 UTC 上传开始小时，格式为 `YYYYMMDDTHH`。 |
| end | String | 是 | - | 包含在内的最后上传小时，整个小时须已结束至少两小时。 |
| schema | Config | 是 | - | 非空的 SeaTunnel JSON 输出模式。 |
| region | String | 否 | `US` | 项目数据所在区域，`US` 或 `EU`。 |
| base_url | String | 否 | 按区域选择 | 可信的 HTTPS 源站，可带尾随斜杠，不允许其他路径。 |
| allow_insecure_http | Boolean | 否 | `false` | 仅用于使用虚拟凭据的隔离 HTTP 测试。 |
| connect_timeout_ms | Int | 否 | `30000` | 建连和连接池等待超时，1..300000 毫秒。 |
| read_timeout_ms | Int | 否 | `120000` | 套接字空闲超时，1..600000 毫秒，不是导出总时限。 |
| max_retries | Int | 否 | `3` | 输出前对网络、429 和 5xx 的额外重试次数，0..10。 |
| retry_backoff_ms | Int | 否 | `1000` | 带抖动的指数退避初始值，1..60000 毫秒。 |
| max_retry_backoff_ms | Int | 否 | `60000` | 最大等待时间，1..600000 毫秒，不能小于初始值。 |
| max_event_bytes | Int | 否 | `1048576` | 不含换行符的 JSONL 单条记录上限，1..16777216 字节。 |
| max_archive_bytes | Long | 否 | `536870912` | 工作节点临时磁盘上下载 ZIP 的字节上限。 |
| max_uncompressed_bytes | Long | 否 | `2147483648` | ZIP 条目和解码后 JSONL 分别累计的字节上限。 |
| field_mapping | Map | 否 | - | 模式列名到 RFC 6901 JSON Pointer 的映射，空指针选择整个事件。 |

`api_key` 和 `secret_key` 为项目的 API 密钥和私密密钥。两个归档大小选项的范围均为
1..68719476736 字节（64 GiB）。默认值有意低于服务限制。不要在缺乏磁盘和处理时间时盲目提高上限，
应缩小导出窗口。连接和读取超时分别限制建连和套接字空闲时间，并非整个导出的总时限。

## 时间窗口和数据可用性

`start` 和 `end` 是 **UTC 上传小时，包含两端**，按 `server_upload_time` 而非
`event_time` 选择事件。例如 `20260101T00` 到 `20260101T01` 包含两个小时。
窗口最多 365 天，日期必须有效。

Amplitude 说明完整小时结束后还需要两小时才能导出，因此结束小时 01:00 最早在 UTC 04:00 被接受。
连接器拒绝较新的窗口，避免将尚未生成的导出误判为空。该边界不保证没有更迟的数据：
满足时间条件的窗口返回 HTTP 404 时表示当前没有可用文件，本次读取以零行结束；
如可能有迟到数据，应重新运行窗口。

一个请求读取整个配置窗口，不自动拆分。服务自身的导出大小限制仍然适用。
HTTP 400 会失败并提示检查窗口和服务限制。大规模历史读取应缩小窗口或使用服务提供的批量导出功能。
多个作业共享项目请求配额。

## 认证和安全

使用 HTTP Basic 认证，API 密钥为用户名，私密密钥为密码，仅通过 Authorization 请求头发送。
US 地址为 `https://amplitude.com/api/2/export`，
EU 为 `https://analytics.eu.amplitude.com/api/2/export`。

`secret_key` 使用 SeaTunnel 现有的敏感配置脱敏规则；项目 `api_key` 不被该规则自动隐藏。
仍需使用引擎的密钥管理和访问控制，不要开启包含真实密钥的 HTTP 请求头或网络日志。
连接器不记录认证头、原始事件或响应体，分片状态不包含密钥或行数据。

`base_url` 会接收认证信息，只能设置为管理员信任的源站，不允许用户信息、查询、片段或额外路径。
默认必须使用 HTTPS，`allow_insecure_http` 仅供使用虚拟凭据的隔离测试。
重定向会失败，不会转发认证头；禁用 Cookie 和 HTTP 客户端隐式重试。
客户端要求 identity HTTP 内容编码并拒绝其他编码；压缩数据应位于导出 ZIP 内，而非额外的 HTTP gzip 层。
响应最多包含 100 个 HTTP 头部，每行最多 8192 个字符。

## 归档验证和资源

ZIP 包含 gzip 压缩的 JSONL 文件，每小时可能有多个文件。先下载到工作节点的私有临时文件，
再验证 ZIP 中央目录、所有 CRC、gzip 尾部和记录大小，最后第二次读取、转换并输出。
不将远端文件名解压为路径，最多接受 10000 个条目和 4 MiB 的中央目录元数据。
创建 ZIP 读取器前先使用有界读取验证目录。每个文件仅允许一个 gzip 成员，拒绝串联成员和尾随数据，
gzip 解码复用仓库管理的 Commons Compress 库。解压后的 ZIP 条目总字节数和 JSONL 总字节数
分别受限。解码器构造前将可选 gzip 头部元数据限制为 64 KiB，拒绝含歧义结束记录、
ZIP64 声明不一致或跨磁盘的归档。验证实际读取字节数，不仅信任 ZIP 声明。

堆内存由单条记录上限和 ZIP 元数据决定，而非导出总大小。
工作线程解析下一条记录时，单槽队列最多保留一行。每个读者可能占用
`max_archive_bytes` 的临时磁盘，输出前额外执行一次完整解压验证。
正常成功、失败和取消均删除临时文件；进程突然终止后仍可能需要操作系统临时目录清理，
不提供跨进程清理或持久归档保证。

截断、压缩损坏、超限、无效 JSON 或类型转换失败都会使作业失败，不会报告成功。
压缩和大小在输出前检查，但后续 JSON/模式错误可能发生在已有部分行输出之后。

## 模式和恢复

每条记录必须为含字符串 `event_type` 的 JSON 对象。其余字段使用现有 JSON 格式的缺失值、
null 和类型转换规则。嵌套属性不自动展平，可使用 ROW/MAP 或 `field_mapping` JSON Pointer；
不存在的指针对应 null，空指针选择整个事件。

恢复会重放 **整个配置窗口**，包括先前已输出的行，不持久化行偏移或完成标记。
这是至少一次重放，不是不可变快照或 Exactly-once。应根据项目的数据契约选择例如 `uuid`
作为下游去重或更新键，不假设导出顺序稳定。

仅在下载阶段、输出任何行之前，对网络错误、429 和 5xx 按 `max_retries` 重试。
指数退避带抖动，并遵守有效的 Retry-After 秒数或 HTTP 日期。
超出 `max_retry_backoff_ms` 时失败，而非提前重试。其他状态码、损坏归档或转换错误不会被静默重试。
检查点锁仅保护行收集和本地完成状态，不覆盖网络与验证等待；取消会终止 HTTP 并唤醒退避。
单个连接器自有线程负责下载、验证和解析，通过单槽队列传递行。
每次轮询最多收集 128 个已就绪的行，不等待工作线程，然后返回引擎，使保存点停止可以关闭读取器。
行仅在轮询线程输出。关闭会中止 I/O、停止并等待该工作线程，最后删除临时文件。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}
source {
  Amplitude {
    api_key = "<AMPLITUDE_PROJECT_API_KEY>"
    secret_key = "<AMPLITUDE_PROJECT_SECRET_KEY>"
    region = US
    start = "20260101T00"
    end = "20260101T01"
    schema {
      fields {
        event_type = string
        uuid = string
        server_upload_time = string
        amount = "decimal(18,2)"
      }
    }
    field_mapping {
      amount = "/event_properties/amount"
    }
  }
}
sink {
  Console {}
}
```

<ChangeLog />

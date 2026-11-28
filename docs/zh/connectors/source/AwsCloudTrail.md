<!--
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
-->

import ChangeLog from '../changelog/connector-http-aws-cloudtrail.md';

# AWS CloudTrail

`AwsCloudTrail` 通过 AWS CloudTrail 区域 [LookupEvents API](https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupEvents.html) 读取
`management_events` 资源。使用 `bin/install-plugin.sh` 安装 `connector-http-aws-cloudtrail`。
使用 SeaTunnel Zeta 批作业，Source 并行度设置为 1，凭据需要 `cloudtrail:LookupEvents` 权限。

Source 读取 AWS 账号的区域性管理事件历史，最多覆盖最近 90 天。S3 中的跟踪导出、CloudTrail Lake 查询、
Insights 和数据事件使用其他 AWS 接口。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `access_key` | string | — | 必填。AWS 访问密钥 ID。 |
| `secret_key` | string | — | 必填。AWS 访问密钥。 |
| `security_token` | string | `""` | 临时凭据的 AWS STS 会话令牌。 |
| `region` | string | `us-east-1` | AWS CloudTrail 控制面区域。 |
| `start_date` | string | 初次作业当天 UTC 零点 | 包含边界的 UTC 日期或带偏移的时间戳。 |
| `end_date` | string | 初次作业启动时刻 | 包含边界的 UTC 日期或带偏移的时间戳。 |
| `lookup_attribute_key` | string | `""` | 可选的服务端过滤键，每次最多一项。 |
| `lookup_attribute_value` | string | `""` | 设置过滤键后必填。 |
| `page_size` | int | `50` | 每页请求记录数，1–50。 |
| `max_pages` | int | `2000` | 一次查询及其恢复最多读取的页数，1–20000。 |
| `max_response_bytes` | int | `8388608` | 传输和解码响应分别使用的字节上限，1024–33554432。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，1–300000。 |
| `read_timeout_ms` | int | `15000` | TLS 握手与响应的空闲超时，1–300000。 |
| `request_timeout_ms` | int | `60000` | 一页的总时限，含节流等待和重试，1–300000。 |
| `max_attempts` | int | `3` | 一页的最大尝试次数，包含初次请求，1–10。 |

使用 AWS 商用区域、GovCloud 和中国区域文档中的标准控制面端点。中国区域的请求使用
`amazonaws.com.cn` 后缀及该分区的凭据。区域同时决定 DNS 端点和签名范围。
[AWS 区域端点](https://docs.aws.amazon.com/general/latest/gr/ct.html)、[AWS 中国区域端点](https://docs.amazonaws.cn/en_us/awscloudtrail/latest/userguide/cloudtrail-supported-regions.html)

支持八种过滤键 `AccessKeyId`、`EventId`、`EventName`、`EventSource`、`ReadOnly`、
`ResourceName`、`ResourceType` 和 `Username`。值按 AWS 规则加权限制为 2000 字符。
下划线、空格、逗号和换行符各算两个字符。[LookupAttribute 契约](https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupAttribute.html)

## 时间范围和恢复

起止时间均包含边界。日期如 `2026-11-01` 表示当天开始的 UTC 零点。
也可用带偏移的整秒时间戳，如 `2026-11-01T10:30:00+08:00`。未配置起始时间时取初次启动
当天的 UTC 零点，未配置结束时间时取初次启动时刻并截到秒。未来的结束时间和超出历史保留期的
起始时间会导致作业失败。

分页和重试使用相同的时间范围与过滤项。短页和空页也可能包含续传令牌。
事件从新到旧输出，同秒事件按实际页内位置继续读取，保留 AWS 重复返回的事件 ID。
达到页数上限且仍有续页时，作业失败并报告查询未完成。

检查点保存固定的时间范围、当前页的不透明续传令牌、已完成页数和未完成页的摘要。
恢复跳过已完成的页，只有重取的记录、顺序及续传令牌与摘要一致才继续此前已输出部分行的页。
检查点绑定 AWS 访问密钥 ID、区域和查询配置。更换密钥 ID 或区域需要启动新查询。
续传令牌或历史窗口过期时作业失败。AWS 未提供跨作业的不可变历史快照。

每个读取器对发送和重试节流，发送频率低于每秒两次。AWS 限额由共享同一账号和区域的所有客户端共同消耗。
HTTPS 使用 AWS Signature Version 4、JVM 信任库及主机名校验。源端异常消息不包含认证凭据或分页令牌。
检查点中可能保存当前分页令牌。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | `management_events`。 |
| `region` | string | 查询该事件的 AWS 区域。 |
| `event_id` | nullable string | AWS 事件 ID。 |
| `event_time` | nullable string | `EventTime` 对应的精确十进制 Unix 秒，`data` 中仍保留数字类型。 |
| `cloud_trail_event` | nullable string | `CloudTrailEvent` 原始文本。 |
| `data` | string | 完整事件的 JSON 文本，保留 `ReadOnly`、`Resources` 等 AWS 响应字段。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  AwsCloudTrail {
    region = "us-east-1"
    access_key = ${CLOUDTRAIL_ACCESS_KEY}
    secret_key = ${CLOUDTRAIL_SECRET_KEY}
    lookup_attribute_key = "ReadOnly"
    lookup_attribute_value = "true"
  }
}
sink {
  Console {}
}
```

## 更新日志

<ChangeLog />

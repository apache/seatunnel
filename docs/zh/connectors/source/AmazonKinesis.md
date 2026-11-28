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

import ChangeLog from '../changelog/connector-amazonkinesis.md';

# Amazon Kinesis Data Streams Source

`AmazonKinesis` 使用 AWS 共享吞吐 API 读取已有 Data Streams 流，每个 Source 配置段
由一个活跃读取器轮询各个可读分片。流式作业定期发现分片调整，子分片在两个仍处于保留期内的
父分片均读完后才开始读取。启用检查点时，父分片的完成状态还须写入成功的检查点。

批作业固定启动时发现的分片集合，读完已关闭的分片。对于开放分片，读取直到收到延迟为零的
空响应。若生产者持续写入，批任务可能达到追赶时限并失败。追赶进度按分片分别计算。

## 配置项

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `region` | String | 是 | — | 流所在 AWS 区域。 |
| `stream_name` | String | 二选一 | — | 凭据所属账户的流名称。 |
| `stream_arn` | String | 二选一 | — | 所选区域内的流 ARN。 |
| `access_key` | String | 显式凭据时 | — | AWS Access Key ID。 |
| `secret_key` | String | 显式凭据时 | — | AWS Secret Access Key。 |
| `token` | String | 显式临时凭据时 | — | AWS 安全令牌。 |
| `start_position` | String | 否 | `TRIM_HORIZON` | `TRIM_HORIZON`、`AT_TIMESTAMP` 或 `LATEST`。 |
| `start_timestamp` | String | `AT_TIMESTAMP` 时 | — | ISO-8601 时间。 |
| `record_limit` | Integer | 否 | `500` | 请求每页条数，1 到 10,000。 |
| `shard_poll_ms` | Integer | 否 | `250` | 同分片请求的最小间隔，200 到 60,000 毫秒。 |
| `discovery_interval_ms` | Integer | 否 | `30000` | 流式分片刷新间隔，1,000 到 600,000 毫秒。 |
| `batch_timeout_ms` | Integer | 否 | `600000` | 批作业单次尝试的追赶时限，1,000 到 86,400,000 毫秒。 |
| `api_timeout_ms` | Integer | 否 | `10000` | SDK 调用及套接字超时，500 到 60,000 毫秒。 |

省略显式密钥时，使用 SeaTunnel Worker 所在环境的 AWS SDK 凭据，包括环境凭据和 AWS
实例或容器凭据。显式临时凭据需要同时设置密钥对和令牌。请为目标流授予
`kinesis:DescribeStreamSummary`、`kinesis:ListShards`、`kinesis:GetShardIterator` 和
`kinesis:GetRecords` 权限。读取加密流时还可能需要 KMS 权限。

## 输出与恢复

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `stream_arn` | STRING | 流 ARN。 |
| `shard_id` | STRING | 分片标识。 |
| `sequence_number` | STRING | 分片内原始序列号。 |
| `partition_key` | STRING | AWS 返回的分区键，可为空。 |
| `arrival_timestamp` | STRING | UTC 格式的近似到达时间，可为空。 |
| `data` | BYTES | Kinesis 原始字节。 |

`LATEST` 固定为协调器初次启动时的时间，并使用时间迭代器。尚未输出过数据的分片恢复后沿用这个
时间。检查点保存每个分片最后成功收集的序列号，恢复时从其后一条记录继续。检查点之后的记录可能重放。
可读数据受 AWS 保留期、时间戳精度和共享限额约束。SeaTunnel 检查点用于恢复同一作业，
独立提交的新作业使用各自的起始位置。

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 1000
}
source {
  AmazonKinesis {
    region = "us-east-1"
    stream_name = "events"
    start_position = "TRIM_HORIZON"
  }
}
sink {
  Console {}
}
```

[GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) ·
[GetShardIterator](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html)

## 变更记录

<ChangeLog />

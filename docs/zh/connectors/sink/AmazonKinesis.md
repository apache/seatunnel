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

# Amazon Kinesis Data Streams Sink

`AmazonKinesis` 将追加型记录写入现有 Data Streams 流。JSON 格式序列化整行，文本格式
将指定字符串列编码为 UTF-8，二进制格式直接写入指定 BYTES 列。

## 配置项

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `region` | String | 是 | — | 流所在 AWS 区域。 |
| `stream_name` | String | 二选一 | — | 凭据所属账户的流名称。 |
| `stream_arn` | String | 二选一 | — | 所选区域内的流 ARN。 |
| `access_key` | String | 显式凭据时 | — | AWS Access Key ID。 |
| `secret_key` | String | 显式凭据时 | — | AWS Secret Access Key。 |
| `token` | String | 显式临时凭据时 | — | AWS 安全令牌。 |
| `format` | String | 否 | `json` | `json`、`text` 或 `binary`。 |
| `data_field` | String | `text` 或 `binary` 时 | — | 包含消息内容的列。 |
| `partition_key_field` | String | 否 | — | 用作分区键的字符串列名。 |
| `partition_key` | String | 否 | 自动生成 | 固定分区键。固定值和字段二选一，均省略时逐条生成。 |
| `batch_size` | Integer | 否 | `100` | 单批最大记录数，1 到 500。 |
| `batch_bytes` | Integer | 否 | `5242880` | 单批消息和分区键的总字节限制，上限 10 MiB。 |
| `record_bytes` | Integer | 否 | `1048576` | 单条消息和分区键的字节限制。仅当目标流已启用大记录时才调高。 |
| `partial_attempts` | Integer | 否 | `4` | 对服务端明确标记可重试的失败条目，尝试 1 到 8 次。 |
| `preserve_order` | Boolean | 否 | `false` | 串行写入，在同一个 Writer 会话中按分区键传递前一条序列号。 |
| `api_timeout_ms` | Integer | 否 | `10000` | SDK 调用与套接字超时，500 到 60,000 毫秒。 |

省略显式密钥时，使用 SeaTunnel Worker 所在环境的 AWS SDK 凭据。显式临时凭据需要密钥对和
令牌。批量模式需要 `kinesis:PutRecords` 权限，有序模式需要 `kinesis:PutRecord` 权限，并且
须设置固定分区键或分区键字段。加密流可能还需要 KMS 权限。

原生 JSON 在构造树之前，按扣除分区键后的记录字节额度限制转换工作量，随后再精确限制编码字节数。数组、Map 和 Row 的每次出现都计入额度，包括重复引用以及转换后键冲突而被覆盖的 Map 条目。因此，转换工作量过大的输入即使最终对象因键覆盖而较小，也会被拒绝。文本先计算 UTF-8 字节数再编码；共享格式化器和检查点格式保持不变。

复合 schema 校验、值转换和 JSON 容器输出均采用迭代方式，仅标量转换器由原生格式化器构造。共享 schema 分支和深而窄的值不会触发复合转换器的递归展开，也不需要新增固定深度上限。数字文本即使规范化后较短，仍会计入输入转换工作量。

## 批量、顺序与恢复

开启检查点后，到达批量阈值、检查点或引擎刷写信号时发送缓存记录。未开启检查点的作业即时
发送。批量响应会完整校验，只有服务端明确标记可重试的失败记录才会重试，原始分区键和消息保持
一致。写入是否成功无法确定时，作业失败。作业恢复可能再次接收已经成功送达 AWS 的旧记录。

AWS 为分片内数据分配顺序。批量重试可能改变记录到达顺序。有序模式在每个 Writer 内串行发送并
保存每个键的前一条序列号，最多跟踪 8,192 个键。多个 Writer、作业恢复、分片调整都可能影响观察到
的顺序。所有字节限制均包含分区键的 UTF-8 字节数。

```hocon
env {
  job.mode = "BATCH"
  checkpoint.interval = 10000
}
source {
  FakeSource {
    schema {
      fields {
        account = "string"
        message = "string"
      }
    }
    rows = [{kind = INSERT, fields = ["account-1", "hello"]}]
  }
}
sink {
  AmazonKinesis {
    region = "us-east-1"
    stream_name = "messages"
    format = "text"
    data_field = "message"
    partition_key_field = "account"
  }
}
```

[PutRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html) ·
[PutRecord](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html)

## 变更记录

<ChangeLog />

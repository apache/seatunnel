import ChangeLog from '../changelog/connector-http-open-data-dc.md';

# Open Data DC

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

`OpenDataDC` 通过 HTTPS 只读访问华盛顿哥伦比亚特区公开发布的
[Crashes in DC](https://catalog.data.gov/dataset/crashes-in-dc) ArcGIS 图层，并读取固定字段。无需身份凭证。
仅支持并行度为 1 的有界批处理，不支持几何对象、关联表和 CDC。

| 参数 | 类型 | 默认值 | 描述 |
| --- | --- | --- | --- |
| `page_size` | Integer | `500` | 每页请求行数，范围为 1 到 1000。 |
| `max_rows` | Long | `0` | 达到指定行数后停止，0 表示读取整个扫描范围。 |
| `query_url` | String | 市政府事故图层 | 用于可信镜像或本地测试服务的 ArcGIS 兼容事故图层 `/query` 地址，不能包含用户名、查询字符串或片段。 |
| `allow_insecure_http` | Boolean | `false` | 仅在可信的本地测试服务中允许明文 HTTP。 |

输出表名为 `crashes`。

| 字段 | SeaTunnel 类型 | 可空 |
| --- | --- | --- |
| OBJECTID | BIGINT | 否 |
| CCN | STRING | 是 |
| REPORTDATE | TIMESTAMP（UTC） | 是 |
| WARD | STRING | 是 |
| TOTAL_VEHICLES | INT | 是 |
| LATITUDE | DOUBLE | 是 |
| LONGITUDE | DOUBLE | 是 |

任务启动时读取最大的 `OBJECTID`，随后按键升序分页。如果 MapServer 报告空页但仍有更多结果，则保持原查询，
按请求的整页大小推进偏移量；收到非空页后恢复按键分页。如果额外读取 32 个分页窗口后仍为空且未结束，任务报错而不是静默截断。检查点保存扫描上界、最后一条已发出的键及行数。
这个过程不是事务快照：扫描期间发布方可能修改或删除记录；两次运行之间也可能重新生成对象 ID。
`OBJECTID` 只用于当前扫描的分页，不应作为长期业务标识。
如果 HTTP 错误触发了任务重试，又没有可恢复的检查点，已读记录可能重复输出。不能接受重复数据时，请使用幂等的下游写入。

遇到 HTTP 429 时仅重试同一个 GET 请求，最多两次。有效且不超过 5 秒的 `Retry-After` 会被遵守；超过 5 秒则失败而非提前重试。
该限制按请求计算，不改变任务级重试可能造成的重放。
Zeta 取消任务时，数据源会中止正在执行的 HTTP 请求，并结束 HTTP 429 重试前的等待。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  OpenDataDC {
    page_size = 2
    max_rows = 4
  }
}
sink {
  Console {}
}
```

## 变更日志

<ChangeLog />

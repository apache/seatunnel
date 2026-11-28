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

import ChangeLog from '../changelog/connector-http-azure-table.md';

# AzureTableStorage

> Azure Table Storage 源连接器 · BATCH

## 描述

使用存储账户访问密钥读取 Azure Table Storage 实体。可以指定表，也可以自动发现账户中的表。各表按 Azure 的分区键和行键顺序读取，实体使用统一的输出结构。单次账户扫描由编号为 0 的子任务执行，建议将源连接器并行度设为 1。

连接器通过 HTTPS 使用 Table 服务的 Shared Key 认证。主机名根据存储账户和 Azure 云环境的域名后缀生成。存储账户需要允许 Shared Key 认证。[Azure Storage Shared Key 认证](https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key)

## 参数

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `account_name` | string | 是 | | Azure 存储账户名，由 3 至 24 个小写字母或数字组成。 |
| `secret_key` | string | 是 | | Base64 编码的存储账户访问密钥。 |
| `endpoint_suffix` | string | 否 | `core.windows.net` | 支持 `core.windows.net`、`core.chinacloudapi.cn`、`core.usgovcloudapi.net`。 |
| `tls_trust_certificate` | string | 否 | | 仅对此连接器生效的 X.509 信任证书，支持 PEM 或 Base64 DER。仍须通过 TLS 主机名校验。 |
| `tables` | list of string | 否 | 全部表 | 设置需要扫描的表名及顺序。空列表表示自动发现。 |
| `start_after_partition_keys` | map of string | 否 | 空 | 按表名设置排他的起始分区键。 |
| `start_after_row_keys` | map of string | 否 | 空 | 按表名设置排他的起始行键。表名须与 `start_after_partition_keys` 一致。 |
| `page_size` | int | 否 | `1000` | 单次请求的最大条数，范围 1 至 1,000。 |
| `max_pages_per_collection` | int | 否 | `10000` | 发现表及扫描每张表分别允许的最大页数。 |
| `max_tables` | int | 否 | `10000` | 允许选择或发现的最大表数量。 |
| `connect_timeout_ms` | int | 否 | `10000` | TCP 连接超时。 |
| `request_timeout_ms` | int | 否 | `30000` | 单次 HTTPS 尝试的时限。 |
| `operation_timeout_ms` | int | 否 | `120000` | 单次读取所有尝试的总时限。 |
| `min_request_interval_ms` | int | 否 | `0` | 各次请求尝试的最短间隔。 |
| `max_retries` | int | 否 | `3` | 暂时性错误的额外尝试次数。 |
| `max_response_bytes` | int | 否 | `4194304` | 单个响应体传输和解码后的字节数上限。 |

## 输出

每个实体输出六个字符串字段。

| 字段 | 值 |
| --- | --- |
| `table_name` | 指定或发现的表名。 |
| `partition_key` | 实体的原始 `PartitionKey`。 |
| `row_key` | 实体的原始 `RowKey`。与表名及分区键共同标识一个实体。 |
| `timestamp` | Azure 维护的 `Timestamp`，保留返回格式。 |
| `etag` | 返回的 OData ETag，服务端省略时为空。 |
| `record` | 包含实体原始字段和 OData 类型注解的 JSON 对象。 |

`record` 保留 Azure EDM 类型注解，包括以字符串表示的 64 位整数、二进制数据、GUID 和日期。缺失字段仍保持缺失。Azure Table Storage 不持久化值为 null 的实体属性。[Table 响应格式](https://learn.microsoft.com/en-us/rest/api/storageservices/payload-format-for-table-service-operations)

## 键范围和恢复

同一表的一对起始键选择分区键大于配置值的实体，以及分区键相同、行键大于配置值的实体。未设置起始键的表执行全量读取。键范围读取仅覆盖指定键之后的实体，无法发现较早键上的更新或插入，也不提供删除记录或基于 `Timestamp` 的变更流。

Azure 可能返回带续页标记的短页或空页。连接器在后续请求中保留初始查询条件。保存点记录表发现进度、当前表、续页标记和已经发出的行位置。恢复时，当前实体页面必须与保存的摘要一致。供应商接口不提供变化数据的快照隔离。[Azure 分页契约](https://learn.microsoft.com/en-us/rest/api/storageservices/query-timeout-and-pagination)

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AzureTableStorage {
    account_name = "mystorageaccount"
    secret_key = ${AZURE_STORAGE_KEY}
    tables = ["Customers", "Orders"]
    start_after_partition_keys = { Orders = "2025" }
    start_after_row_keys = { Orders = "000128" }
  }
}

sink {
  LocalFile {
    path = "/data/azure-table-export"
    file_format_type = "json"
  }
}
```

<ChangeLog />

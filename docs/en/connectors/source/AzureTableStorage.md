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

> Azure Table Storage source · BATCH

## Description

Read entities from Azure Table Storage using a storage account access key. List tables explicitly or let the source discover the account's tables. Each table is read in Azure's partition-key and row-key order, and all entities share one output schema. The source runs one account scan on subtask zero. Use parallelism 1 for this source.

The connector uses Table service Shared Key authentication over HTTPS. The host is derived from the storage account and the Azure cloud suffix. The account must allow Shared Key authorization. [Azure Storage Shared Key authentication](https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key)

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `account_name` | string | yes | | Azure Storage account name, 3 to 24 lowercase letters or digits. |
| `secret_key` | string | yes | | Base64 storage account access key. |
| `endpoint_suffix` | string | no | `core.windows.net` | Supported suffixes are `core.windows.net`, `core.chinacloudapi.cn`, and `core.usgovcloudapi.net`. |
| `tls_trust_certificate` | string | no | | Optional connector-local X.509 trust anchor in PEM or base64 DER. TLS hostname verification still applies. |
| `tables` | list of string | no | all tables | Table names in scan order. An empty list discovers all tables. |
| `start_after_partition_keys` | map of string | no | empty | Exclusive starting partition key by table name. |
| `start_after_row_keys` | map of string | no | empty | Exclusive starting row key by table name. Use the same table names as in `start_after_partition_keys`. |
| `page_size` | int | no | `1000` | Maximum items per request, from 1 to 1,000. |
| `max_pages_per_collection` | int | no | `10000` | Individual page limit for discovery and for each table. |
| `max_tables` | int | no | `10000` | Maximum number of selected or discovered tables. |
| `connect_timeout_ms` | int | no | `10000` | TCP connect timeout. |
| `request_timeout_ms` | int | no | `30000` | Deadline for one HTTPS attempt. |
| `operation_timeout_ms` | int | no | `120000` | Deadline across all attempts for one read. |
| `min_request_interval_ms` | int | no | `0` | Minimum interval between attempts. |
| `max_retries` | int | no | `3` | Additional attempts after transient errors. |
| `max_response_bytes` | int | no | `4194304` | Limit for both encoded and decoded response bytes. |

## Output

Each entity produces six string columns.

| Column | Value |
| --- | --- |
| `table_name` | Name of the selected or discovered table. |
| `partition_key` | The entity's exact `PartitionKey`. |
| `row_key` | The entity's exact `RowKey`. Together with the partition key and table name, it identifies an entity. |
| `timestamp` | The Azure-maintained `Timestamp` in the returned format. |
| `etag` | The returned OData ETag, or null when Azure omits it. |
| `record` | Original entity fields and OData type annotations as a JSON object. |

Azure's EDM annotations remain in `record`, including the string representation of 64-bit integers, binary, GUID and date values. Missing properties stay absent. Azure Table Storage does not persist null entity properties. [Table payload formats](https://learn.microsoft.com/en-us/rest/api/storageservices/payload-format-for-table-service-operations)

## Key ranges and recovery

A pair of configured start keys selects entities whose partition key is greater than the configured partition, or whose partition key is equal and row key is greater. Tables without a pair use a full scan. A key range reads entities after the given key. It cannot observe changes or insertions at earlier keys or deleted entities, and it does not use `Timestamp` as a change stream.

Azure may return a short or empty page with a continuation. The source keeps the initial query when following it. A savepoint preserves table discovery, the current table, provider continuations and the accepted row position. On restore, the current entity page must match its saved digest. The service does not provide snapshot isolation for changing table data. [Azure continuation contract](https://learn.microsoft.com/en-us/rest/api/storageservices/query-timeout-and-pagination)

## Example

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

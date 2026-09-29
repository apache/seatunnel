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

---
name: file_etl
description: File-based ETL — reading from or writing to file systems (S3, HDFS, local, FTP)
triggers:
  - file
  - s3
  - S3
  - hdfs
  - HDFS
  - csv
  - json file
  - parquet
  - orc
  - avro
  - ftp
  - sftp
  - local file
  - oss
  - gcs
  - blob
  - bucket
tools:
  - get_connector_info
  - list_connectors
composable: true
---

## When to Use

User wants to read from or write to file-based systems: S3, HDFS, local filesystem,
FTP/SFTP, OSS, GCS, Azure Blob. Includes scenarios like "export MySQL to S3 as parquet"
or "load CSV files into database".

## Domain Knowledge

- File connectors in SeaTunnel: `S3File`, `HdfsFile`, `LocalFile`, `FtpFile`, `SftpFile`, `OssFile`.
- File connectors support multiple formats: `json`, `csv`, `parquet`, `orc`, `avro`, `text`, `excel`, `xml`.
- The `file_format_type` option specifies the format (REQUIRED for file connectors).
- For file sources, `path` specifies where to read from.
- For file sinks, `path` specifies the output directory, `file_name_expression` controls naming.
- S3 requires: `bucket`, `path`, `access_key` / `secret_key` (or `${ENV_VAR}`), `fs.s3a.endpoint`.
- HDFS requires: `path` (hdfs://...), optionally `fs.defaultFS`.
- CSV format options: `delimiter`, `skip_header_row_number`.
- When reading files as source, `schema` must be defined to specify field names and types.
- File sinks often need `sink_columns` to control which fields are written.

## SOP

1. **Identify file connector** (S3File, HdfsFile, LocalFile, etc.) and its role (source or sink).
2. **Determine file format**: csv, json, parquet, orc, avro, etc.
3. **Fill file-specific options**: path, bucket, credentials, format options.
4. **If file is SOURCE**: define `schema { fields { ... } }` with field names and types.
5. **If file is SINK**: set `path`, `file_format_type`, optionally `file_name_expression`.
6. **Fill the other end** (database connector or another file connector) with its options.
7. **Set routing** via plugin_output/plugin_input.
8. **Validate**: path format correct, required credentials present, schema defined for source.

## Constraints

- File sources MUST define `schema` with field types — files don't have intrinsic schema (except parquet/orc which have embedded schema).
- S3 credentials always as `${AWS_ACCESS_KEY}` / `${AWS_SECRET_KEY}`.
- `file_format_type` is REQUIRED — never omit it.
- Path format must match the connector: `s3://bucket/path` for S3, `hdfs://host/path` for HDFS, `/absolute/path` for local.

## Pattern

```hocon
env {
  parallelism = <parallelism>
  job.mode = "BATCH"
}

source {
  <FileConnector> {
    path = "<file_path>"
    file_format_type = "<format>"
    schema {
      fields {
        <field_name> = "<type>"
      }
    }
    plugin_output = "<routing_label>"
  }
}

sink {
  <SinkConnector> {
    <sink_options>
    plugin_input = "<routing_label>"
  }
}
```

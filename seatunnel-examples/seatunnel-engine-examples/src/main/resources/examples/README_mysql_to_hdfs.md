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

# Batch Offline ETL: MySQL to HDFS (Partitioned Parquet)

## 1. Scenario Overview

This example demonstrates a typical batch offline ingestion pipeline from MySQL to an HDFS data warehouse using Apache SeaTunnel:

- **Source (`Jdbc`)**: Reads source orders data from MySQL with parallel split optimization (`partition_column`, `partition_num`, bounds).
- **Transform (`Sql`)**: Cleanses data and shapes the schema:
  - Renames primary key column (`id` -> `order_id`).
  - Normalizes order status to uppercase (`upper(status) as order_status`).
  - Generates a date partition key from timestamp (`date_format(create_time, 'yyyy-MM-dd') as pt_dt`).
  - Filters out anomalous rows (`where amount >= 0`).
- **Sink (`HdfsFile`)**: Writes data into HDFS in snappy-compressed Parquet format using Hive-style dynamic partitioning (`/pt_dt=YYYY-MM-DD/`).

---

## 2. Prerequisites

1. **Install Connector Plugins**:
   Ensure `connector-jdbc` and `connector-file-hadoop` plugins are installed:
   ```bash
   cd "${SEATUNNEL_HOME}"
   sh bin/install-plugin.sh
   ```

2. **Add Required Jars**:
   Place the following driver jars into `${SEATUNNEL_HOME}/lib/`:
   - MySQL JDBC Driver (e.g., `mysql-connector-j-8.x.jar`)
   - Hadoop client dependencies corresponding to your Hadoop cluster version.

---

## 3. Step-by-Step Guide

### Step 1: Prepare Source Database and Data

Execute `trade_db_orders.sql` in MySQL to initialize the schema and sample records:

```bash
mysql -h 127.0.0.1 -P 3306 -u test_user -p < trade_db_orders.sql
```

The sample dataset contains 5 orders across two days (`2026-08-23` and `2026-08-24`), including mixed lowercase/uppercase statuses and a negative amount for filter validation.

### Step 2: Configure Job

Review `mysql_to_hdfs_batch_dw.conf`:
- Update `url`, `user`, and `password` under `source.Jdbc` to match your MySQL environment.
- Update `fs.defaultFS` and `path` under `sink.HdfsFile` to point to your HDFS NameNode and warehouse directory.

### Step 3: Run the SeaTunnel Job

Execute the job using SeaTunnel Zeta in local or cluster mode:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/mysql_to_hdfs_batch_dw.conf -m local
```

---

## 4. Expected Output & Partition Layout

### Dynamic Partition Directory Tree

SeaTunnel automatically routes rows into corresponding partition subdirectories according to `partition_by = ["pt_dt"]`:

```text
/user/hive/warehouse/dwd.db/dwd_orders_df/
├── pt_dt=2026-08-23/
│   ├── e2e_xxxx_0.snappy.parquet
│   └── e2e_xxxx_1.snappy.parquet
└── pt_dt=2026-08-24/
    ├── e2e_xxxx_0.snappy.parquet
    └── e2e_xxxx_1.snappy.parquet
```

### Data Verification

1. **Partition Routing**:
   - Orders dated `2026-08-23` are written into `pt_dt=2026-08-23/`.
   - Orders dated `2026-08-24` are written into `pt_dt=2026-08-24/`.
2. **Transform Effects**:
   - `order_status` values are normalized to uppercase (e.g., `COMPLETED`, `PENDING`, `PAID`).
   - Order with negative amount (`id = 5`, `amount = -10.00`) is filtered out.
3. **Format & Compression**:
   - All data files are standard Parquet files compressed with Snappy.

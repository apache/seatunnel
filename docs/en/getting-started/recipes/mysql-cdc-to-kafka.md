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

# MySQL CDC to Kafka with metadata headers and field shaping

This page keeps the scope intentionally narrow: it documents the exact MySQL CDC to Kafka job that was validated end to end in Docker on July 16, 2026.

The verified pipeline is:

- `MySQL-CDC` reads the snapshot and later binlog changes from `shop.orders`
- `Metadata` exposes database, table, and row kind as normal fields
- `Sql` renames business fields and fills custom fields
- `Kafka` writes JSON payloads while moving selected metadata into Kafka headers

## Validation environment prerequisites

The validated Docker run had these prerequisites in place before the job started:

- MySQL ran with the GTID/binlog settings from `docker/server-gtids/my.cnf`.
- The MySQL JDBC driver JAR was present under the `MySQL-CDC` plugin `lib` directory.
- A CDC user named `st_user_source` existed with `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, `REPLICATION CLIENT`, and `LOCK TABLES` privileges.
- Kafka topic `recipe_mysql_orders` was created before the SeaTunnel job started.

## Verified source data

The E2E test created this MySQL table and loaded two initial rows:

```sql
CREATE TABLE orders (
  id BIGINT NOT NULL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  user_id BIGINT NOT NULL,
  status INT NOT NULL,
  amount DECIMAL(10, 2) NOT NULL
);

INSERT INTO orders (id, order_no, user_id, status, amount) VALUES
  (1001, 'ORD-1001', 501, 0, 19.99),
  (1002, 'ORD-1002', 502, 1, 29.99);
```

After the job started, the test executed these incremental changes:

```sql
UPDATE shop.orders SET status = 2, amount = 39.99 WHERE id = 1001;

INSERT INTO shop.orders (id, order_no, user_id, status, amount) VALUES
  (1003, 'ORD-1003', 503, 0, 59.99);
```

## Exact job config validated in Docker

The following config is the exact job config that was validated in the Docker test environment. The hostnames are Docker network aliases from that environment.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "mysql_orders_raw"
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    username = "st_user_source"
    password = "mysqlpw"
    server-id = 5601-5604
    table-names = ["shop.orders"]
    startup.mode = "initial"
    schema-changes.enabled = false
  }
}

transform {
  Metadata {
    plugin_input = "mysql_orders_raw"
    plugin_output = "mysql_orders_with_meta"
    metadata_fields {
      Database = source_database
      Table = source_table
      RowKind = change_type
    }
  }

  Sql {
    plugin_input = "mysql_orders_with_meta"
    plugin_output = "kafka_orders"
    query = "select id as order_id, order_no, user_id, amount, case when status = 0 then 'CREATED' when status = 1 then 'PAID' when status = 2 then 'SHIPPED' else 'OTHER' end as status_name, source_database, source_table, change_type, CONCAT(source_database, '.', source_table) as source_name, 'mysql_cdc' as sync_source from dual where id is not null"
  }
}

sink {
  Kafka {
    plugin_input = "kafka_orders"
    bootstrap.servers = "kafkaCluster:9092"
    topic = "recipe_mysql_orders"
    format = json
    partition_key_fields = ["order_id"]
    kafka_headers_fields = ["source_database", "source_table", "change_type"]
  }
}
```

## What the validation proved

The Docker E2E test asserted all of the following:

- The snapshot phase produced Kafka records for the initial MySQL rows.
- Record `1001` was written with payload fields `order_id`, `status_name`, `source_name`, and `sync_source`.
- On snapshot record `1001`, Kafka headers contained `source_database=shop`, `source_table=orders`, and `change_type=+I`.
- On the same snapshot record, those metadata fields were removed from the JSON payload after `kafka_headers_fields` was applied.
- After `UPDATE shop.orders SET status = 2 ... WHERE id = 1001`, the latest record for `1001` carried `change_type=+U` and `status_name=SHIPPED`.
- After inserting `1003`, the latest record for `1003` carried `change_type=+I` and `status_name=CREATED`.

## Why each step exists in this recipe

### 1. `Metadata` exposes CDC fields that the sink can reuse

This validated job only exposed the fields that were later consumed by the Kafka sink or SQL transform:

- `source_database`
- `source_table`
- `change_type`

### 2. `Sql` keeps business shaping in one place

In this validated run, the SQL produced these observed results:

- row `1001` was written with `status_name=CREATED` during the snapshot phase
- row `1002` was written with `status_name=PAID` during the snapshot phase
- row `1001` later changed to `status_name=SHIPPED` after the update
- row `1003` was written with `status_name=CREATED` after the insert
- the asserted records also included `sync_source`, and snapshot row `1001` included `source_name`

### 3. `kafka_headers_fields` changes both headers and payload

This behavior was asserted on the snapshot record for row `1001`. After these three fields were moved into Kafka headers:

- `source_database`
- `source_table`
- `change_type`

they were no longer present in that JSON payload.

## Related docs

- [`MySQL-CDC` source connector](../../connectors/source/MySQL-CDC.md)
- [`Kafka` sink connector](../../connectors/sink/Kafka.md)
- [`Metadata` transform](../../transforms/metadata.md)
- [`Sql` transform](../../transforms/sql.md)

---
sidebar_position: 1
---

# CDC Production Cookbook: Full + Incremental Synchronization

## Overview

This cookbook covers production-grade configuration and operation of Change Data Capture (CDC)
connectors in SeaTunnel. It supplements the per-connector parameter references with end-to-end
examples for the most common target systems and a troubleshooting checklist.

**Supported CDC sources covered in this guide:**

| Source | Connector |
|---|---|
| MySQL | [MySQL-CDC](source/MySQL-CDC.md) |
| PostgreSQL | [PostgreSQL-CDC](source/PostgreSQL-CDC.md) |
| Oracle | [Oracle-CDC](source/Oracle-CDC.md) |

---

## 1. Full + Incremental Synchronization Lifecycle

A CDC job runs in two sequential phases:

```
Phase 1: Full Snapshot (batch read)
  ──> Read existing rows from every table in parallel splits
  ──> Write all rows to sink (no 2PC required for snapshot rows)
  ──> Record the binlog/WAL/SCN position at snapshot start

Phase 2: Incremental CDC (streaming)
  ──> Tail binlog/WAL/logminer from the snapshot-start position
  ──> Convert INSERT/UPDATE/DELETE events to SeaTunnelRow with RowKind
  ──> Write to sink via 2PC (if sink supports exactly-once)
```

This lifecycle is controlled by `startup.mode`:

| `startup.mode` | Behavior |
|---|---|
| `initial` | Run full snapshot, then switch to incremental (recommended for production) |
| `earliest` | Skip snapshot, start from the oldest available binlog/WAL position |
| `latest` | Skip snapshot, start from the current binlog/WAL position |
| `specific` | Start from a user-specified binlog file+offset or LSN/SCN |
| `timestamp` | Start from the binlog/WAL position at a given timestamp |

**Production recommendation**: always use `startup.mode=initial` unless you are resuming a failed
incremental job. Starting from `latest` silently skips existing data.

---

## 2. Prerequisites by Database

### MySQL CDC

```
1. Enable binlog:
   binlog_format = ROW
   binlog_row_image = FULL

2. Create a CDC user with replication privileges:
   CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'password';
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

3. (Optional but recommended) Enable GTID for automatic failover:
   gtid_mode = ON
   enforce_gtid_consistency = ON
```

**`server-id` requirement**: MySQL treats each replication client as a unique server. The `server-id`
in your job config must not conflict with any other MySQL replica or SeaTunnel CDC job connecting
to the same MySQL instance. Assign a unique range (e.g., 5400–5499) per job.

### PostgreSQL CDC

```
1. Set the WAL level:
   wal_level = logical

2. Create a replication slot:
   SELECT pg_create_logical_replication_slot('seatunnel_slot', 'pgoutput');

3. Grant replication privilege:
   ALTER ROLE cdc_user REPLICATION LOGIN;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;

4. Publication (pgoutput decoder):
   CREATE PUBLICATION seatunnel_pub FOR ALL TABLES;
```

### Oracle CDC

```
1. Enable supplemental logging:
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

2. Enable LogMiner:
   EXECUTE DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);

3. Create LogMiner user:
   CREATE USER logminer_user IDENTIFIED BY password;
   GRANT CREATE SESSION, LOGMINING, SELECT ANY TRANSACTION TO logminer_user;
   GRANT SELECT ON V_$LOGMNR_CONTENTS TO logminer_user;
```

---

## 3. Production Examples

### 3.1 MySQL CDC → Apache Doris

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql-host:3306/orders_db"
    username = "cdc_user"
    password = "password"
    database-names = ["orders_db"]
    table-names = ["orders_db.orders", "orders_db.order_items"]
    startup.mode = "initial"
    server-id = "5400-5404"
    # Enable exactly-once: coordinate 2PC with Doris sink
    exactly-once = true
  }
}

sink {
  Doris {
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "orders_dw"
    table = "${table_name}"
    sink.enable-2pc = true
    sink.label-prefix = "seatunnel_cdc_orders"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

**Key points:**
- `sink.enable-2pc = true` coordinates with SeaTunnel's exactly-once checkpoint
- `checkpoint.interval` controls both the CDC commit frequency and the 2PC commit interval
- A shorter interval reduces latency but increases Doris label metadata overhead

---

### 3.2 MySQL CDC → StarRocks

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql-host:3306/inventory"
    username = "cdc_user"
    password = "password"
    database-names = ["inventory"]
    table-names = ["inventory.products", "inventory.customers"]
    startup.mode = "initial"
    server-id = "5410-5414"
    exactly-once = true
  }
}

sink {
  StarRocks {
    nodeUrls = ["starrocks-fe:8030"]
    username = "root"
    password = ""
    database = "inventory_dw"
    table = "${table_name}"
    sink.properties.format = "json"
    sink.properties.strip_outer_array = "true"
    # Enable transaction for exactly-once
    enable_upsert_delete = true
  }
}
```

---

### 3.3 MySQL CDC → Kafka

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql-host:3306/app_db"
    username = "cdc_user"
    password = "password"
    database-names = ["app_db"]
    table-names = ["app_db.users", "app_db.events"]
    startup.mode = "initial"
    server-id = "5420-5424"
    format = "COMPATIBLE_DEBEZIUM_JSON"
  }
}

sink {
  Kafka {
    topic = "cdc.${database_name}.${table_name}"
    bootstrap.servers = "kafka:9092"
    kafka.config {
      acks = "all"
      # Enable Kafka transactions for exactly-once
      transactional.id = "seatunnel-cdc-${table_name}"
    }
  }
}
```

**Debezium-compatible format**: setting `format = COMPATIBLE_DEBEZIUM_JSON` produces Kafka messages
that downstream consumers (Flink, Kafka Connect, etc.) can process with standard Debezium schemas.

---

### 3.4 PostgreSQL CDC → JDBC / Doris / StarRocks

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 20000
}

source {
  Postgres-CDC {
    base-url = "jdbc:postgresql://pg-host:5432/mydb"
    username = "cdc_user"
    password = "password"
    database-names = ["mydb"]
    schema-names = ["public"]
    table-names = ["public.orders", "public.customers"]
    startup.mode = "initial"
    slot.name = "seatunnel_slot"
    decoding.plugin.name = "pgoutput"
  }
}

sink {
  Doris {
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "mydb_ods"
    table = "${table_name}"
    sink.enable-2pc = true
    sink.label-prefix = "seatunnel_pg_cdc"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

---

### 3.5 Oracle CDC → Doris / StarRocks / JDBC

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Oracle-CDC {
    base-url = "jdbc:oracle:thin:@oracle-host:1521:ORCL"
    username = "logminer_user"
    password = "password"
    database-names = ["ORCL"]
    schema-names = ["HR"]
    table-names = ["HR.EMPLOYEES", "HR.DEPARTMENTS"]
    startup.mode = "initial"
  }
}

sink {
  Doris {
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "hr_ods"
    table = "${table_name}"
    sink.enable-2pc = true
    sink.label-prefix = "seatunnel_oracle_cdc"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

---

## 4. Checkpoint and 2PC Interaction

The `checkpoint.interval` setting controls both fault-tolerance frequency and the 2PC commit
cadence for sinks like Doris and StarRocks:

```
Checkpoint N triggers:
  1. Source: flush current binlog offset to checkpoint storage
  2. Sink: commit all pending micro-batch transactions (2PC phase 2)
  3. Sink: begin new transaction for next batch (2PC phase 1)

On failure and restore:
  1. Engine restores binlog offset from last completed checkpoint
  2. Sink rolls back any in-flight (uncommitted) transactions
  3. CDC replay continues from checkpoint offset — no data loss, no duplicates
```

**Choosing `checkpoint.interval`:**

| Scenario | Recommended interval |
|---|---|
| Low-latency CDC (< 5 s freshness required) | 5 000–10 000 ms |
| Standard production CDC | 30 000–60 000 ms |
| Oracle LogMiner (heavy query overhead) | 60 000–120 000 ms |
| High-throughput bulk CDC | 60 000–300 000 ms |

---

## 5. Schema Evolution and DDL Support Boundaries

| DDL Operation | MySQL CDC | PostgreSQL CDC | Oracle CDC |
|---|---|---|---|
| ADD COLUMN | Supported (since 2.3.x) | Supported | Limited |
| DROP COLUMN | Not propagated by default | Not propagated | Not propagated |
| RENAME COLUMN | Not supported | Not supported | Not supported |
| ALTER COLUMN TYPE | Risky — may cause deserialization errors | Risky | Risky |
| TRUNCATE TABLE | Not captured | Not captured | Not captured |
| DROP TABLE | Not captured | Not captured | Not captured |

**Production recommendation**: avoid `ALTER TABLE` on tables being synced by CDC without a
coordinated schema migration plan. For complex DDL changes, use the following procedure:

1. Stop the CDC job gracefully with a savepoint
2. Apply DDL to both source and target
3. Update the job config if schema mapping changes
4. Restart the job from the savepoint

---

## 6. Observing CDC Delay

### Query running job metrics via REST API

```bash
# Get current CDC lag (record count behind)
curl http://<master>:8080/hazelcast/rest/maps/running-job-metrics/<job-id>
```

### Key metrics to watch

| Metric | Meaning |
|---|---|
| `SourceReceivedCount` | Total records read from source (snapshot + incremental) |
| `SinkWriteCount` | Total records written to sink |
| `lag` | Binlog offset lag in MySQL terms |

### Lag monitoring script

```bash
#!/bin/bash
JOB_ID=$1
MASTER=$2
while true; do
  echo "$(date) --- CDC lag:"
  curl -s http://$MASTER:8080/hazelcast/rest/maps/running-job-metrics/$JOB_ID | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d)"
  sleep 30
done
```

---

## 7. Troubleshooting Checklist

### Permission errors

| Error | Fix |
|---|---|
| `Access denied for user ... REPLICATION` | Grant `REPLICATION SLAVE` and `REPLICATION CLIENT` to CDC user |
| `pg_hba.conf` replication rejected | Add `host replication cdc_user 0.0.0.0/0 md5` to pg_hba.conf |
| Oracle `ORA-01031: insufficient privileges` | Grant `LOGMINING`, `SELECT ANY TRANSACTION` to LogMiner user |

### Network issues

| Error | Fix |
|---|---|
| `Connection refused` on binlog port | Verify MySQL `bind-address` is not `127.0.0.1`; check firewall |
| PostgreSQL replication slot not created | Verify `wal_level = logical` and restart PG; slot creation requires superuser or replication role |

### Replication / offset issues

| Error | Fix |
|---|---|
| `Could not find first log file name in binary log` | MySQL binlog rotation deleted the starting position; reset job with `startup.mode=initial` |
| `Replication slot ... does not exist` | Slot was dropped; re-create it and restart job with `startup.mode=initial` |
| `SCN not found in redo log` | Oracle redo log recycled the starting SCN; restart job with `startup.mode=initial` |

### Checkpoint failures

| Error | Fix |
|---|---|
| `Checkpoint timeout exceeded` | Increase `checkpoint.timeout`; reduce `parallelism` to reduce checkpoint overhead |
| `Checkpoint storage path not accessible` | Verify HDFS / S3 connectivity from all workers; check storage credentials |
| `state not found on restore` | Checkpoint was deleted or path changed; submit fresh job |

### 2PC sink issues

| Error | Fix |
|---|---|
| Doris `Label already exists` | Duplicate label due to restart without savepoint; change `sink.label-prefix` |
| StarRocks transaction timeout | Increase `sink.properties.timeout`; check StarRocks FE connection pooling |

---

## See Also

- [MySQL CDC Connector Reference](source/MySQL-CDC.md)
- [PostgreSQL CDC Connector Reference](source/PostgreSQL-CDC.md)
- [Oracle CDC Connector Reference](source/Oracle-CDC.md)
- [Zeta State Storage and Recovery](../engines/zeta/state-storage-and-recovery.md)
- [REST API Job Lifecycle Cookbook](../engines/zeta/rest-api-job-lifecycle.md)

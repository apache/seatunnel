---
sidebar_position: 5
title: File to StarRocks
---

# File to StarRocks

Use this recipe when you want to import local CSV or text files into StarRocks for fast analytical queries.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md).

2. Install the plugins required by this recipe. Follow [Deployment > Download The Connector Plugins](../locally/deployment.md#download-the-connector-plugins), then keep only the plugins below in `config/plugin_config`:

```plugin_config
--seatunnel-connectors--
connector-file-local
connector-starrocks
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(file-local|starrocks)'
```

3. Put the MySQL JDBC driver required by the StarRocks sink into `${SEATUNNEL_HOME}/lib`, then confirm the jar is visible:

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector'
```

4. Prepare the local input file and make sure the SeaTunnel process can read it:

```bash
mkdir -p /tmp/seatunnel/input
cat <<'EOF' > /tmp/seatunnel/input/customers.csv
id,name,city,updated_at
1001,Alice,Shanghai,2026-06-12 10:00:00
1002,Bob,Beijing,2026-06-12 10:05:00
1003,Carol,Hangzhou,2026-06-12 10:10:00
EOF
```

5. Create the target database and table in StarRocks before running the job.

## Minimal configuration

This example reads a local CSV file with a header line and writes the rows to an existing StarRocks primary-key table.

Prepare the target table first:

```sql
CREATE DATABASE IF NOT EXISTS sync_demo;

CREATE TABLE IF NOT EXISTS sync_demo.customers (
  id BIGINT NOT NULL,
  name STRING,
  city STRING,
  updated_at DATETIME
)
ENGINE=OLAP
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
  "replication_num" = "1"
);
```

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    plugin_output = "customers_file"
    path = "/tmp/seatunnel/input/customers.csv"
    file_format_type = "csv"
    csv_use_header_line = true
    schema = {
      fields {
        id = bigint
        name = string
        city = string
        updated_at = timestamp
      }
    }
  }
}

sink {
  StarRocks {
    plugin_input = "customers_file"
    nodeUrls = ["starrocks-fe:8030"]
    base-url = "jdbc:mysql://starrocks-fe:9030/sync_demo"
    username = "root"
    password = ""
    database = "sync_demo"
    table = "customers"
    batch_max_rows = 1000
    schema_save_mode = "IGNORE"
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}
```

## Run the job

Save the config as `config/file-to-starrocks.conf`, then run SeaTunnel in local mode:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/file-to-starrocks.conf -m local
```

## Validation result

1. Run the job and confirm it finishes without StarRocks stream load errors.
2. Check the target table in StarRocks.

```sql
SELECT COUNT(*) FROM sync_demo.customers;
SELECT id, name, city, updated_at FROM sync_demo.customers ORDER BY id;
```

If the imported rows in StarRocks match the file content, the pipeline is working.

## Common pitfalls

- `base-url` is missing even though `nodeUrls` is configured.
- The file has a header row, but `csv_use_header_line = true` is not set.
- The source schema does not match the file delimiter or timestamp format.
- The target table was not created before the job. This recipe uses `schema_save_mode = "IGNORE"` because the local file source does not provide primary-key metadata for StarRocks auto DDL.

## Related docs

- [LocalFile source](../../connectors/source/LocalFile.md)
- [StarRocks sink](../../connectors/sink/StarRocks.md)

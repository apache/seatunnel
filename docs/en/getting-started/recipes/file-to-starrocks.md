---
sidebar_position: 5
title: File to StarRocks
---

# File to StarRocks

Use this recipe when you want to import local CSV or text files into StarRocks for fast analytical queries.

## Prerequisites

- Finish [Run your first job](../locally/run-your-first-job.md).
- Install the `connector-file-local` and `connector-starrocks` plugins.
- Put the MySQL JDBC driver required by the StarRocks sink into `${SEATUNNEL_HOME}/lib`.
- Prepare a local input file that is accessible to the SeaTunnel process.

## Minimal configuration

This example reads a local CSV file with a header line and writes the rows to StarRocks.

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
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}
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
- The StarRocks table model or auto-created schema does not match the intended query pattern.

## Related docs

- [LocalFile source](../../connectors/source/LocalFile.md)
- [StarRocks sink](../../connectors/sink/StarRocks.md)

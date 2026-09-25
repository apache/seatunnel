---
title: MySQL to HDFS
---

# MySQL to HDFS

Use this recipe to batch-load MySQL orders into HDFS as Snappy-compressed Parquet files, partitioned by date. The pipeline uses a JDBC source, a SQL transform, and an HdfsFile sink.

The transform renames `id` to `order_id`, normalizes order status to uppercase, filters negative amounts, and derives the `pt_dt` partition key. This is a batch snapshot example, not CDC or an incremental synchronization job.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md). This recipe uses SeaTunnel Zeta in local mode on Linux. Set `SEATUNNEL_HOME` to your extracted SeaTunnel distribution directory; a source checkout is not required.
2. Install `connector-jdbc` and `connector-file-hadoop` for the same SeaTunnel version as the distribution. Follow [Deployment](../locally/deployment.md) and include the following entries in `config/plugin_config`. Preserve any other connectors your environment needs.

```plugin_config
--seatunnel-connectors--
connector-jdbc
connector-file-hadoop
--end--
```

3. Put the MySQL JDBC driver, such as `mysql-connector-j-8.x.jar`, in `${SEATUNNEL_HOME}/lib`. Install the connectors and check that they and the driver are present:

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | grep -E 'connector-(jdbc|file-hadoop)'
ls lib | grep 'mysql-connector'
```

The Zeta distribution includes Hadoop jars; inspect `lib` before adding dependencies. Do not mix arbitrary Hadoop client versions. See [HdfsFile sink](../../connectors/sink/HdfsFile.md) for environment-specific requirements.

4. Prepare an accessible MySQL instance and an account with `SELECT` permission on the source table. Use a setup account with database/table creation and insert permissions for the seed SQL below; the job account does not need these setup permissions.
5. Prepare a reachable HDFS cluster and an unused output directory. The SeaTunnel process must have permission to write both the output and the sink's temporary directory (default `/tmp/seatunnel`). The example assumes non-Kerberos HDFS; for Kerberos or HA, configure the additional options from the HdfsFile documentation. The Hadoop CLI used for validation must also be configured to access that cluster.

## Prepare source data

:::caution Use an isolated test database

Run the SQL below once in a MySQL client using a setup account. It intentionally uses `CREATE DATABASE` without `IF NOT EXISTS` and does not drop or truncate any table. If `trade_db` already exists, stop and choose an unused test database name; replace it consistently in the SQL, JDBC URL, `table_path`, and queries. Do not force the SQL client to continue after errors.

:::

```sql
CREATE DATABASE trade_db;
USE trade_db;

CREATE TABLE orders (
  id BIGINT NOT NULL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  user_id BIGINT NOT NULL,
  amount DECIMAL(10, 2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  create_time DATETIME NOT NULL
);

INSERT INTO orders (id, order_no, user_id, amount, status, create_time) VALUES
  (1, 'ORD-20260823-001', 10001, 99.50, 'completed', '2026-08-23 10:15:30'),
  (2, 'ORD-20260823-002', 10002, 199.00, 'pending', '2026-08-23 14:20:00'),
  (3, 'ORD-20260824-001', 10003, 49.90, 'COMPLETED', '2026-08-24 09:00:15'),
  (4, 'ORD-20260824-002', 10001, 350.00, 'paid', '2026-08-24 18:45:10'),
  (5, 'ORD-20260824-003', 10004, -10.00, 'cancelled', '2026-08-24 20:00:00');
```

The five orders span two dates and include one negative amount to demonstrate filtering. Configure the job account's access to this test table before running SeaTunnel.

## Complete configuration

Save the following as `config/mysql-to-hdfs.conf` under your SeaTunnel distribution.

Replace the JDBC host, database name, `username`, and `password` with your test environment values. Replace `fs.defaultFS` and `path` with your HDFS address and unused test output path. Here, `localhost` means the machine running SeaTunnel, and `namenode` must be resolvable from that machine. The sample credentials are placeholders; use your environment's required TLS settings outside this isolated example.

```hocon
env {
  job.name = "mysql_to_hdfs_batch_dw"
  job.mode = "BATCH"
  parallelism = 4
}

source {
  Jdbc {
    plugin_output = "src_mysql_orders"
    url = "jdbc:mysql://localhost:3306/trade_db?useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "test_user"
    password = "test_password"

    table_path = "trade_db.orders"
    query = "select id, order_no, user_id, amount, status, create_time, date(create_time) as create_date from trade_db.orders"

    partition_column = "id"
    partition_num = 4
    partition_lower_bound = 1
    partition_upper_bound = 10000000
    fetch_size = 2000
  }
}

transform {
  Sql {
    plugin_input = "src_mysql_orders"
    plugin_output = "dwd_orders"
    query = """
      select
        id as order_id,
        order_no,
        user_id,
        amount,
        upper(status) as order_status,
        create_time,
        FORMATDATETIME(create_date, 'yyyy-MM-dd') as pt_dt
      from src_mysql_orders
      where amount >= 0
    """
  }
}

sink {
  HdfsFile {
    plugin_input = "dwd_orders"
    fs.defaultFS = "hdfs://namenode:8020"
    path = "/user/hive/warehouse/dwd.db/dwd_orders_df"
    file_format_type = "parquet"
    partition_by = ["pt_dt"]
    compress_codec = "snappy"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

The JDBC query evaluates `date(create_time)` in MySQL. The SQL transform then uses SeaTunnel's `FORMATDATETIME(create_date, 'yyyy-MM-dd')` to build the partition key. Keep `plugin_input` and `plugin_output` consistent between plugins.

The partition bounds and `partition_num` illustrate JDBC split-read configuration, not performance tuning for five rows. Adapt them to the real source data when scaling up; a tiny dataset need not use all writers or produce equally sized files.

## Run the job

Before the first run, record the expected counts in MySQL:

```sql
SELECT DATE(create_time) AS pt_dt, COUNT(*) AS expected_rows
FROM trade_db.orders
WHERE amount >= 0
GROUP BY DATE(create_time)
ORDER BY pt_dt;
```

For the supplied seed data, expect two rows for each date, four rows in total. Then submit the job:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/mysql-to-hdfs.conf -m local
```

Wait for the job to finish successfully before checking output.

:::caution Repeated runs append data

This job uses `APPEND_DATA`: rerunning it against the same source and output path can add duplicate records. The four-row expectation applies to one successful run into an unused output directory. Use a new test output path for another validation run; do not delete or overwrite existing warehouse data.

:::

## Validation result

### Check the partition directories

Run the following with your actual HDFS address and output path:

```bash
hdfs dfs -ls -R hdfs://namenode:8020/user/hive/warehouse/dwd.db/dwd_orders_df
```

The expected layout is:

```text
/user/hive/warehouse/dwd.db/dwd_orders_df/
├── pt_dt=2026-08-23/
│   └── <generated-file>.parquet
└── pt_dt=2026-08-24/
    └── <generated-file>.parquet
```

The tree is illustrative: the actual names and number of data files depend on writer instances, parallelism, and data distribution. A filename alone does not prove the compression codec.

### Check the records and file format

Using a Parquet-capable reader already available in your environment, read all committed data files in both partition directories. For the supplied seed data and a single run, verify these values (row order is not guaranteed):

| order_id | order_status | amount | pt_dt |
| --- | --- | --- | --- |
| 1 | COMPLETED | 99.50 | 2026-08-23 |
| 2 | PENDING | 199.00 | 2026-08-23 |
| 3 | COMPLETED | 49.90 | 2026-08-24 |
| 4 | PAID | 350.00 | 2026-08-24 |

Also verify that `order_no`, `user_id`, and `create_time` are preserved. Order `5` must be absent because its amount is negative. Inspect the Parquet metadata to confirm Snappy compression; do not use `cat` to interpret the binary files.

The `pt_dt` value identifies the directory partition. Depending on the reader, it may need to be inferred from the directory rather than read as a column from an individual file. Hive-style directories do not automatically create or register a Hive table. Directory existence alone is not a complete data validation.

## Common pitfalls

- The MySQL JDBC driver is missing from the SeaTunnel process's `lib` directory, or connector versions do not match the distribution.
- `localhost` points to the wrong machine, or the HDFS NameNode/DataNode hostnames are unreachable from SeaTunnel.
- The job account can connect to MySQL but cannot read `trade_db.orders`, or the HDFS identity cannot write output or temporary files.
- The tutorial is run against an existing database or output directory, making the seed data or expected row counts invalid.
- Changing the source query removes `create_date` while the SQL transform still references it.
- A Parquet file is treated as plain text, or a fixed filename/file count is assumed.

## Related docs

- [JDBC source](../../connectors/source/Jdbc.md)
- [SQL transform](../../transforms/sql.md)
- [SQL functions](../../transforms/sql-functions.md)
- [HdfsFile sink](../../connectors/sink/HdfsFile.md)

---
sidebar_position: 2
title: JDBC to S3
---

# JDBC to S3

Use this recipe when you want to export table data from a relational database to an S3-compatible object store.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md).

2. Install the plugins required by this recipe. Follow [Deployment > Download The Connector Plugins](../locally/deployment.md#download-the-connector-plugins), then keep only the plugins below in `config/plugin_config`:

```plugin_config
--seatunnel-connectors--
connector-jdbc
connector-file-s3
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(jdbc|file-s3)'
```

3. Put the source database JDBC driver into `${SEATUNNEL_HOME}/lib` for SeaTunnel Zeta, then confirm the jar is visible:

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector'
```

4. Put `hadoop-aws` and the AWS SDK bundle required by the S3 connector into `${SEATUNNEL_HOME}/lib`, then confirm both jars are present:

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'hadoop-aws|aws-java-sdk-bundle'
```

5. Prepare the source table in the relational database. This example uses MySQL and exports two rows from `analytics.orders`:

```sql
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  total_amount DECIMAL(16, 2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO analytics.orders (id, customer_id, total_amount, updated_at) VALUES
  (5001, 101, 19.99, NOW()),
  (5002, 102, 29.99, NOW());
```

6. Prepare an S3 bucket and credentials with write permission. The example below assumes the bucket already exists and that `access_key` and `secret_key` can write to `s3://company-data-lake/seatunnel/orders/`.

## Minimal configuration

This example exports a query result from MySQL to S3 in JSON lines format so that the output is easy to inspect.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    plugin_output = "orders_jdbc"
    url = "jdbc:mysql://mysql:3306/analytics"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "password"
    query = "select id, customer_id, total_amount, updated_at from orders"
  }
}

sink {
  S3File {
    plugin_input = "orders_jdbc"
    bucket = "s3a://company-data-lake"
    path = "/seatunnel/orders/"
    fs.s3a.endpoint = "s3.us-east-1.amazonaws.com"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    file_format_type = "json"
    row_delimiter = "\n"
    custom_filename = true
    file_name_expression = "orders"
    filename_extension = "json"
    single_file_mode = true
    is_enable_transaction = false
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## Run the job

Save the config as `config/jdbc-to-s3.conf`, then run SeaTunnel in local mode:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/jdbc-to-s3.conf -m local
```

## Validation result

1. Run the source query directly in the database and record the expected row count.
2. Start the SeaTunnel job.
3. Verify that new objects are written to the expected S3 prefix.

```bash
aws s3 ls s3://company-data-lake/seatunnel/orders/ --recursive
aws s3 cp s3://company-data-lake/seatunnel/orders/orders.json - | head
```

If objects are created under the target prefix and the exported content matches the source query, the pipeline is working.

## Common pitfalls

- The JDBC driver is available on your workstation but not under `${SEATUNNEL_HOME}/lib`.
- `bucket` and `path` are mixed up. Keep the bucket in `bucket` and the prefix in `path`.
- The credential provider does not match the authentication method you configured.
- Large tables are exported through one unbounded query without filtering or partitioning.
- Fixed filenames are only safe for this single-file tutorial. If you enable transactions again, keep `${transactionId}` in `file_name_expression`.
- The target endpoint is S3-compatible, but the `fs.s3a.endpoint` value still points to AWS.

## Related docs

- [JDBC source](../../connectors/source/Jdbc.md)
- [S3File sink](../../connectors/sink/S3File.md)

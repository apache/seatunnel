---
sidebar_position: 2
title: JDBC to S3
---

# JDBC to S3

Use this recipe when you want to export table data from a relational database to an S3-compatible object store.

## Prerequisites

- Finish [Run your first job](../locally/run-your-first-job.md).
- Install the `connector-jdbc` and `connector-file-s3` plugins.
- Put the source database JDBC driver into `${SEATUNNEL_HOME}/lib` for SeaTunnel Zeta.
- Put `hadoop-aws` and the AWS SDK bundle required by the S3 connector into `${SEATUNNEL_HOME}/lib`.
- Prepare an S3 bucket and credentials with write permission.

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

---
sidebar_position: 2
title: JDBC 到 S3
---

# JDBC 到 S3

当你想把关系型数据库里的表数据批量导出到 S3 或兼容 S3 的对象存储时，可以使用这条链路。

## 前置条件

- 先完成 [跑第一个任务](../locally/run-your-first-job.md)。
- 安装 `connector-jdbc` 和 `connector-file-s3`。
- 把源端数据库 JDBC 驱动放到 `${SEATUNNEL_HOME}/lib`。
- 把 S3 连接器依赖的 `hadoop-aws` 和 AWS SDK bundle 放到 `${SEATUNNEL_HOME}/lib`。
- 准备好可写入的 S3 bucket 和访问凭据。

## 最小配置

这个示例把 MySQL 查询结果导出成 JSON Lines，便于直接检查输出内容。

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
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## 验证结果

1. 先在数据库里直接执行源查询，记录预期行数。
2. 启动 SeaTunnel 作业。
3. 检查目标 S3 前缀下是否生成了新对象。

```bash
aws s3 ls s3://company-data-lake/seatunnel/orders/ --recursive
aws s3 cp s3://company-data-lake/seatunnel/orders/part-00000.json - | head
```

如果目标前缀下生成了对象，且内容和源查询结果一致，这条链路就是通的。

## 常见坑

- JDBC 驱动在本机有，但没有放进 `${SEATUNNEL_HOME}/lib`。
- `bucket` 和 `path` 写反了。`bucket` 写桶，`path` 写桶内前缀。
- 凭据提供器和你实际配置的认证方式不匹配。
- 大表直接跑一个无边界 `query`，没有做过滤或分片。
- 目标是兼容 S3 的对象存储，但 `fs.s3a.endpoint` 还在指向 AWS 默认地址。

## 相关文档

- [JDBC Source](../../connectors/source/Jdbc.md)
- [S3File Sink](../../connectors/sink/S3File.md)

---
sidebar_position: 4
title: Http 到 JDBC
---

# Http 到 JDBC

当你想从 HTTP API 拉取结构化数据，并把结果落到关系型数据库中时，可以使用这条链路。

## 前置条件

- 先完成 [跑第一个任务](../locally/run-your-first-job.md)。
- 安装 `connector-http` 和 `connector-jdbc`。
- 把目标数据库 JDBC 驱动放到 `${SEATUNNEL_HOME}/lib`。
- 确认 HTTP API 返回结构稳定。如果有效数据嵌套较深，要提前规划 `json_field` 或 `content_field`。

## 最小配置

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Http {
    plugin_output = "http_orders"
    url = "http://mockserver:1080/example/http"
    method = "GET"
    format = "json"
    schema = {
      fields {
        c_string = string
        c_int = int
      }
    }
  }
}

sink {
  Jdbc {
    plugin_input = "http_orders"
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://postgresql:5432/test?loggerLevel=OFF"
    username = "test"
    password = "test"
    generate_sink_sql = true
    database = "test"
    table = "public.http_orders"
    primary_keys = ["c_string"]
    batch_size = 100
  }
}
```

## 验证结果

1. 运行任务，确认没有 HTTP 解析错误和 JDBC DDL 错误。
2. 查询目标表，核对行数和 API 返回结果。

```sql
SELECT COUNT(*) FROM public.http_orders;
SELECT c_string, c_int FROM public.http_orders ORDER BY c_string;
```

如果目标表里的数据和 HTTP 返回内容一致，这条链路就是通的。

## 常见坑

- 返回体是 JSON，但 schema 中字段名或字段类型写错了。
- API 数据是嵌套结构，但没有配置 `content_field` 或 `json_field`。
- 源接口有分页或限流，但作业按单页接口处理。
- JDBC sink 虽然自动建表了，但你选的主键并不能真正唯一标识一条记录。

## 相关文档

- [Http Source](../../connectors/source/Http.md)
- [JDBC Sink](../../connectors/sink/Jdbc.md)

---
sidebar_position: 4
title: Http 到 JDBC
---

# Http 到 JDBC

当你想从 HTTP API 拉取结构化数据，并把结果落到关系型数据库中时，可以使用这条链路。

## 前置条件

1. 先完成 [跑第一个任务](../locally/run-your-first-job.md)。

2. 安装这条链路需要的插件。先看 [部署 > 下载连接器插件](../locally/deployment.md#下载连接器插件)，然后把 `config/plugin_config` 改成下面这样：

```plugin_config
--seatunnel-connectors--
connector-http-base
connector-jdbc
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(http-base|jdbc)'
```

3. 把目标数据库 JDBC 驱动放进 `${SEATUNNEL_HOME}/lib`，并确认 jar 已经落盘：

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'postgresql'
```

4. 运行任务前，先看一眼 HTTP 返回内容。这里直接使用 [Http Source](../../connectors/source/Http.md) 里的示例接口，返回 JSON 顶层应该能看到 `c_string` 和 `c_int` 这些字段：

```bash
curl http://mockserver:1080/example/http
```

如果你的真实接口把有效数据包在更深层字段里，就要先补 `json_field` 或 `content_field`，否则别急着运行。

5. 先准备 PostgreSQL 目标库，并给 sink 用户授予在 `public` schema 自动建表的权限，因为这篇教程使用了 `generate_sink_sql = true`：

```sql
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE test OWNER test;
```

重新连接到 `test` 库以后，再执行：

```sql
GRANT USAGE, CREATE ON SCHEMA public TO test;
```

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

## 运行任务

把配置保存为 `config/http-to-jdbc.conf`，然后用本地模式运行 SeaTunnel：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/http-to-jdbc.conf -m local
```

## 验证结果

1. 运行任务，确认没有 HTTP 解析错误和 JDBC DDL 错误。
2. 查询目标表，核对行数和 API 返回结果。

```sql
SELECT COUNT(*) FROM public.http_orders;
SELECT c_string, c_int FROM public.http_orders ORDER BY c_string;
```

如果目标表里的数据和 HTTP 返回内容一致，这条链路就是通的。使用默认 mock 返回时，查询结果里应该能看到和 `curl` 输出一致的 `c_string`、`c_int` 值。

## 常见坑

- 返回体是 JSON，但 schema 中字段名或字段类型写错了。
- API 数据是嵌套结构，但没有配置 `content_field` 或 `json_field`。
- 源接口有分页或限流，但作业按单页接口处理。
- JDBC sink 虽然自动建表了，但你选的主键并不能真正唯一标识一条记录。

## 相关文档

- [Http Source](../../connectors/source/Http.md)
- [JDBC Sink](../../connectors/sink/Jdbc.md)

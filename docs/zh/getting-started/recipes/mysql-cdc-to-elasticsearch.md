---
title: MySQL CDC 到 Elasticsearch：过滤、转换与自定义字段
---

# MySQL CDC 到 Elasticsearch：过滤、清洗与自定义补字段

本教程把客户资料从 MySQL 持续同步到 Elasticsearch，同时过滤非管理范围的数据、清洗手机号、映射状态值并补充来源元数据。

完整链路是：

- `MySQL-CDC` 读取 `crm.customer_profile`
- `Metadata` 暴露库名、表名和行变更类型
- `Replace` 去掉手机号中的 `-`
- `Sql` 负责行过滤、状态映射和补充 `sync_source`
- `Elasticsearch` 按主键把数据写入索引

## 前置条件

1. 先完成 [跑第一个任务](../locally/run-your-first-job.md)，确认本地执行链路正常。

2. 按照 [部署 > 下载连接器插件](../locally/deployment.md#下载连接器插件) 安装本教程需要的连接器，并在 `config/plugin_config` 中保留下面两项：

```plugin_config
--seatunnel-connectors--
connector-cdc-mysql
connector-elasticsearch
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | grep -E 'connector-(cdc-mysql|elasticsearch)'
```

3. 使用 Zeta 引擎时，把 MySQL Connector/J 放入 `${SEATUNNEL_HOME}/lib`，并确认 JAR 已经落盘：

```bash
ls "${SEATUNNEL_HOME}/lib" | grep 'mysql-connector'
```

4. 创建 CDC 账号，不授予修改表结构的权限：

```sql
CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
ON *.* TO 'st_user_source'@'%';
FLUSH PRIVILEGES;
```

5. 检查 MySQL binlog 是否满足 CDC 要求：

```sql
SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');
```

期望值是 `log_bin = ON`、`binlog_format = ROW`、`binlog_row_image = FULL`。

6. 确保 Elasticsearch 可以通过 HTTPS 认证访问。本示例使用 Elasticsearch 8.9.0。请使用 SeaTunnel 能访问的地址和账号检查连通性：

```bash
curl --cacert /path/to/http_ca.crt \
  -u elastic:your_password https://elasticsearch.example.com:9200
```

把可信 CA 证书复制到每个 SeaTunnel 节点，并在任务配置的 `tls_truststore_path` 中填写对应的本地路径。

## 准备源表数据

创建源表并插入两条初始数据：

```sql
CREATE DATABASE IF NOT EXISTS crm;
USE crm;

CREATE TABLE customer_profile (
  id BIGINT NOT NULL PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  phone VARCHAR(32) NOT NULL,
  email VARCHAR(128) NOT NULL,
  status INT NOT NULL,
  city VARCHAR(64) NOT NULL
);

INSERT INTO customer_profile (id, name, phone, email, status, city) VALUES
  (1001, ' Alice Zhang ', '138-0000-1111', 'alice@example.com', 1, 'Shanghai'),
  (900, 'Bob Li', '139-8888-2222', 'bob@example.com', 0, 'Beijing');
```

任务启动且初始快照已经可以在 Elasticsearch 中查询到后，执行下面两条增量变更：

```sql
UPDATE crm.customer_profile
SET name = ' Alice Zhang ', phone = '138-9999-0000', status = 2
WHERE id = 1001;

INSERT INTO crm.customer_profile (id, name, phone, email, status, city) VALUES
  (1003, 'Carol Wang', '137-1234-8888', 'carol@example.com', 1, 'Hangzhou');
```

## 完整任务配置

请把下面的示例主机名和凭据替换为 SeaTunnel 进程可以访问的实际值。并发运行的 MySQL CDC 任务必须使用不同的 `server-id` 范围。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "mysql_customer_raw"
    url = "jdbc:mysql://mysql.example.com:3306/crm"
    username = "st_user_source"
    password = "mysqlpw"
    server-id = 5701-5704
    table-names = ["crm.customer_profile"]
    startup.mode = "initial"
    schema-changes.enabled = false
  }
}

transform {
  Metadata {
    plugin_input = "mysql_customer_raw"
    plugin_output = "mysql_customer_with_meta"
    metadata_fields {
      Database = source_database
      Table = source_table
      RowKind = row_kind
    }
  }

  Replace {
    plugin_input = "mysql_customer_with_meta"
    plugin_output = "mysql_customer_cleaned"
    replace_fields = ["phone"]
    pattern = "-"
    replacement = ""
    is_regex = false
  }

  Sql {
    plugin_input = "mysql_customer_cleaned"
    plugin_output = "es_customer_profile"
    query = "select id, trim(name) as name, phone, email, city, case when status = 1 then 'ACTIVE' when status = 2 then 'FROZEN' else 'OTHER' end as status_name, source_database, source_table, row_kind, 'mysql_cdc' as sync_source from dual where id >= 1000"
  }
}

sink {
  Elasticsearch {
    plugin_input = "es_customer_profile"
    hosts = ["https://elasticsearch.example.com:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = true
    tls_verify_hostname = true
    tls_truststore_path = "/path/to/http_ca.crt"
    index = "recipe_customer_profile"
    primary_keys = ["id"]
    max_batch_size = 1
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

把调整后的配置保存为 `${SEATUNNEL_HOME}/config/mysql-cdc-to-elasticsearch.conf`，然后使用 Zeta local 模式提交：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/mysql-cdc-to-elasticsearch.conf -m local
```

在另一个终端中，分别在执行上面的增量 SQL 前后查询索引：

```bash
curl --cacert /path/to/http_ca.crt -u elastic:your_password \
  'https://elasticsearch.example.com:9200/recipe_customer_profile/_search?pretty&sort=id'
```

## 验证结果

在 Elasticsearch 返回结果中确认以下内容：

- 快照阶段最终只写入了 1 条文档。
- `1001` 被保留下来，因为它的不可变主键属于 `id >= 1000` 的管理范围。
- `900` 被过滤掉，因为它不在这个主键范围内。
- `trim(name)` 把 `' Alice Zhang '` 变成了 `Alice Zhang`。
- `Replace` 把 `138-0000-1111` 变成了 `13800001111`。
- 初始写入文档里包含 `status_name=ACTIVE`、`source_database=crm`、`source_table=customer_profile`、`sync_source=mysql_cdc`。
- 更新 `1001` 后，ES 中这条文档变成 `status_name=FROZEN`、`phone=13899990000`、`row_kind=+U`。
- 插入 `1003` 后，ES 中新增第二条文档，字段包括 `status_name=ACTIVE`、`phone=13712348888`、`row_kind=+I`、`sync_source=mysql_cdc`。

## 为什么这条链路这样写

### 1. `Replace` 负责轻量清洗

这份配置里，`Replace` 只做一件事：把手机号里的 `-` 去掉。

### 2. `Sql` 负责业务过滤与补字段

SQL 包含以下业务规则：

- 只保留 `id >= 1000` 的主键范围
- 把 `status` 映射成 `ACTIVE` 或 `FROZEN`
- 新增常量字段 `sync_source`

这里特意使用不可变主键做过滤。不要在写入 upsert sink 之前直接用 `is_deleted`、`status` 这类可变字段过滤 CDC 更新：已经写入 ES 的记录如果后来不再满足条件，旧文档不会自动收到删除事件。软删除场景应把删除标记同步到 Elasticsearch 后在查询时过滤，或者使用能把状态变化明确转换为删除事件的链路。

### 3. Elasticsearch 写入配置

Sink 使用下面这些参数，并产生上述预期结果：

- `primary_keys = ["id"]`
- `max_batch_size = 1`
- `tls_verify_certificate = true` 和 `tls_verify_hostname = true`
- `schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"`
- `data_save_mode = "APPEND_DATA"`

## 相关文档

- [`MySQL-CDC` source 连接器](../../connectors/source/MySQL-CDC.md)
- [`Elasticsearch` sink 连接器](../../connectors/sink/Elasticsearch.md)
- [`Metadata` transform](../../transforms/metadata.md)
- [`Replace` transform](../../transforms/replace.md)
- [`Sql` transform](../../transforms/sql.md)

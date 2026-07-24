---
title: MySQL CDC to Elasticsearch with filtering, conversion, and custom fields
---

# MySQL CDC to Elasticsearch with filtering, cleanup, and custom fields

This recipe continuously synchronizes customer profiles from MySQL to Elasticsearch, filters unmanaged rows, normalizes phone numbers, maps status values, and adds source metadata.

The pipeline is:

- `MySQL-CDC` reads `crm.customer_profile`
- `Metadata` exposes database, table, and row kind
- `Replace` removes `-` from the phone number
- `Sql` filters rows and derives `status_name` plus `sync_source`
- `Elasticsearch` writes documents by primary key

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md) and make sure local execution works.

2. Install the connectors required by this recipe. Follow [Deployment > Download The Connector Plugins](../locally/deployment.md#download-the-connector-plugins), then keep these entries in `config/plugin_config`:

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

3. For the Zeta engine, place MySQL Connector/J in `${SEATUNNEL_HOME}/lib` and verify that the JAR is visible:

```bash
ls "${SEATUNNEL_HOME}/lib" | grep 'mysql-connector'
```

4. Create the CDC account without granting schema-changing privileges:

```sql
CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
ON *.* TO 'st_user_source'@'%';
FLUSH PRIVILEGES;
```

5. Verify that MySQL binlog is ready for CDC:

```sql
SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');
```

The expected values are `log_bin = ON`, `binlog_format = ROW`, and `binlog_row_image = FULL`.

6. Make Elasticsearch reachable with HTTPS authentication. This example uses Elasticsearch 8.9.0. Verify connectivity with an address and account that SeaTunnel can use:

```bash
curl --cacert /path/to/http_ca.crt \
  -u elastic:your_password https://elasticsearch.example.com:9200
```

Copy the trusted CA certificate to every SeaTunnel node and use that local path for `tls_truststore_path` in the job configuration.

## Prepare the source data

Create the source table and load two initial rows:

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

After the job starts and the initial snapshot is visible in Elasticsearch, execute these incremental changes:

```sql
UPDATE crm.customer_profile
SET name = ' Alice Zhang ', phone = '138-9999-0000', status = 2
WHERE id = 1001;

INSERT INTO crm.customer_profile (id, name, phone, email, status, city) VALUES
  (1003, 'Carol Wang', '137-1234-8888', 'carol@example.com', 1, 'Hangzhou');
```

## Complete job configuration

Replace the example hostnames and credentials below with values reachable from the SeaTunnel process. Each concurrently running MySQL CDC job must use a unique `server-id` range.

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

Save the adapted config as `${SEATUNNEL_HOME}/config/mysql-cdc-to-elasticsearch.conf`, then submit it with the Zeta local engine:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/mysql-cdc-to-elasticsearch.conf -m local
```

In another terminal, query the indexed documents before and after executing the incremental SQL above:

```bash
curl --cacert /path/to/http_ca.crt -u elastic:your_password \
  'https://elasticsearch.example.com:9200/recipe_customer_profile/_search?pretty&sort=id'
```

## Verify the result

Confirm the following results in the Elasticsearch response:

- Only one document existed after the snapshot phase.
- Document `1001` was kept because its immutable primary key is in the managed range `id >= 1000`.
- Row `900` was filtered out because it is outside that primary-key range.
- `trim(name)` changed `' Alice Zhang '` into `Alice Zhang`.
- `Replace` changed `138-0000-1111` into `13800001111`.
- The indexed document contained `status_name=ACTIVE`, `source_database=crm`, `source_table=customer_profile`, and `sync_source=mysql_cdc`.
- After the update, document `1001` became `status_name=FROZEN`, `phone=13899990000`, and `row_kind=+U`.
- After inserting `1003`, Elasticsearch contained a second document with `status_name=ACTIVE`, `phone=13712348888`, `row_kind=+I`, and `sync_source=mysql_cdc`.

## Why each step exists in this recipe

### 1. `Replace` is used for lightweight field normalization

This job uses `Replace` only for one purpose: remove `-` from `phone` before indexing.

### 2. `Sql` owns the business filter

The SQL contains the full business rule:

- keep only the managed primary-key range `id >= 1000`
- map `status` to `ACTIVE` or `FROZEN`
- add constant field `sync_source`

The filter intentionally uses the immutable primary key. Avoid filtering CDC updates by mutable fields such as `is_deleted` or `status` before an upsert sink: when a previously indexed row stops matching, no delete event is emitted for the old Elasticsearch document. For soft deletion, keep the deletion flag in Elasticsearch and filter it at query time, or use a pipeline that explicitly converts the transition into a delete event.

### 3. Elasticsearch write settings

The sink uses these settings together with the expected results above:

- `primary_keys = ["id"]`
- `max_batch_size = 1`
- `tls_verify_certificate = true` and `tls_verify_hostname = true`
- `schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"`
- `data_save_mode = "APPEND_DATA"`

## Related docs

- [`MySQL-CDC` source connector](../../connectors/source/MySQL-CDC.md)
- [`Elasticsearch` sink connector](../../connectors/sink/Elasticsearch.md)
- [`Metadata` transform](../../transforms/metadata.md)
- [`Replace` transform](../../transforms/replace.md)
- [`Sql` transform](../../transforms/sql.md)

---
sidebar_position: 4
title: HTTP to JDBC
---

# HTTP to JDBC

Use this recipe when you want to pull structured data from an HTTP API and store the result in a relational database.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md).

2. Install the plugins required by this recipe. Follow [Deployment > Download The Connector Plugins](../locally/deployment.md#download-the-connector-plugins), then keep only the plugins below in `config/plugin_config`:

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

3. Put the target database JDBC driver into `${SEATUNNEL_HOME}/lib`, then confirm the jar is visible:

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'postgresql'
```

4. Inspect the HTTP response before running the job. The sample endpoint from the [Http source](../../connectors/source/Http.md) should return a JSON body that contains top-level fields such as `c_string` and `c_int`:

```bash
curl http://mockserver:1080/example/http
```

If your real API nests the useful records under another field, define `json_field` or `content_field` before you continue.

5. Prepare the PostgreSQL target database and grant the sink user permission to create tables in `public`, because this recipe uses `generate_sink_sql = true`:

```sql
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE test OWNER test;
```

Reconnect to database `test`, then run:

```sql
GRANT USAGE, CREATE ON SCHEMA public TO test;
```

## Minimal configuration

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

## Run the job

Save the config as `config/http-to-jdbc.conf`, then run SeaTunnel in local mode:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/http-to-jdbc.conf -m local
```

## Validation result

1. Run the job and confirm there are no HTTP parse or JDBC DDL errors.
2. Query the target table and compare the row count with the API response.

```sql
SELECT COUNT(*) FROM public.http_orders;
SELECT c_string, c_int FROM public.http_orders ORDER BY c_string;
```

If the rows in the target table match the HTTP response, the pipeline is working. With the default mock response, you should see the same `c_string` and `c_int` values you saw in `curl`.

## Common pitfalls

- The response body is JSON, but the configured schema does not match the actual field names or types.
- The API data is nested, but `content_field` or `json_field` is not configured.
- Pagination or rate limits exist on the source API, but the job treats it as a single-page endpoint.
- The JDBC sink auto-creates a table, but the chosen primary key does not uniquely identify records.

## Related docs

- [Http source](../../connectors/source/Http.md)
- [JDBC sink](../../connectors/sink/Jdbc.md)

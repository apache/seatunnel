---
sidebar_position: 4
title: Http to JDBC
---

# Http to JDBC

Use this recipe when you want to pull structured data from an HTTP API and store the result in a relational database.

## Prerequisites

- Finish [Run your first job](../locally/run-your-first-job.md).
- Install the `connector-http` and `connector-jdbc` plugins.
- Put the target database JDBC driver into `${SEATUNNEL_HOME}/lib`.
- Make sure the HTTP API returns a stable JSON structure, or define `json_field` or `content_field` if the useful records are nested.

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

## Validation result

1. Run the job and confirm there are no HTTP parse or JDBC DDL errors.
2. Query the target table and compare the row count with the API response.

```sql
SELECT COUNT(*) FROM public.http_orders;
SELECT c_string, c_int FROM public.http_orders ORDER BY c_string;
```

If the rows in the target table match the HTTP response, the pipeline is working.

## Common pitfalls

- The response body is JSON, but the configured schema does not match the actual field names or types.
- The API data is nested, but `content_field` or `json_field` is not configured.
- Pagination or rate limits exist on the source API, but the job treats it as a single-page endpoint.
- The JDBC sink auto-creates a table, but the chosen primary key does not uniquely identify records.

## Related docs

- [Http source](../../connectors/source/Http.md)
- [JDBC sink](../../connectors/sink/Jdbc.md)

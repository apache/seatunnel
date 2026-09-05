# Sink Options Placeholders

## Introduction

The SeaTunnel provides a sink options placeholders feature that allows you to get upstream table metadata through placeholders.

This functionality is essential when you need to dynamically get upstream table metadata (such as multi-table writes).

This document will guide you through the usage of these placeholders and how to leverage them effectively.

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## Placeholder

The placeholders are mainly controlled by the following expressions:

- `${database_name}`
  - Used to get the database in the upstream catalog table
  - Default values can also be specified via expressions：`${database_name:default_my_db}`
- `${schema_name}`
  - Used to get the schema in the upstream catalog table
  - Default values can also be specified via expressions：`${schema_name:default_my_schema}`
- `${table_name}`
  - Used to get the table in the upstream catalog table
  - Default values can also be specified via expressions：`${table_name:default_my_table}`
- `${schema_full_name}`
  - Used to get the schema full path(database & schema) in the upstream catalog table
- `${table_full_name}`
  - Used to get the table full path(database & schema & table) in the upstream catalog table
- `${primary_key}`
  - Used to get the table primary-key fields in the upstream catalog table
- `${unique_key}`
  - Used to get the table unique-key fields in the upstream catalog table
- `${field_names}`
  - Used to get the table field keys in the upstream catalog table
- `${comment}`
  - Used to get the table comment in the upstream catalog table
- `${partition_keys}`
  - Used to get the table partition keys in the upstream catalog table

## Configuration

*Requires*:
- Make sure the sink connector you are using has implemented `TableSinkFactory` API

### Example 1

```hocon
env {
  // ignore...
}
source {
  MySQL-CDC {
    // ignore...
  }
}

transform {
  // ignore...
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"

    database = "${database_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

### Example 2

```hocon
env {
  // ignore...
}
source {
  Oracle-CDC {
    // ignore...
  }
}

transform {
  // ignore...
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"

    database = "${schema_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```
## Primary Key Placeholder Behavior

The `${primary_key}` placeholder expands to all primary key columns defined in the upstream table metadata.

For example, if the upstream table primary key is:

```text
(f1, f2)
```

then:

```hocon
primary_keys = ["${primary_key}"]
```

will be expanded to:

```hocon
primary_keys = ["f1", "f2"]
```

Mixing `${primary_key}` with static column names is currently not supported.

For example, the following configuration is **not supported**:

```hocon
primary_keys = ["${primary_key}", "tenant_id"]
```

The placeholder replacement for list values is only applied when `${primary_key}` is the only element in the list.

The restriction above applies to the top-level `primary_keys` option. The JDBC sink additionally supports
mixing `${primary_key}` (and `${unique_key}`) with static column names inside
`multi-table_config.primary_keys`; see the JDBC sink documentation for details.

The behavior is the same for both single-table and multi-table jobs.

We will complete the placeholder replacement before the connector is started, ensuring that the sink options is ready before use.
If the variable is not replaced, it may be that the upstream table metadata is missing this option, for example:
- `mysql` source not contain `${schema_name}`
- `oracle` source not contain `${database_name}`
- ...

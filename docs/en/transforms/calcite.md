# Calcite

> Calcite SQL transform plugin

## Description

SQL transform plugin powered by [Apache Calcite](https://calcite.apache.org/). Use standard SQL to transform data rows. The SQL plan is compiled once at job startup and applied to each row at runtime.

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| sql | string | yes | - | SQL statement to execute. The `FROM` table name must match `plugin_input` |
| table_transform | list | no | [] | Per-table SQL overrides for multi-table CDC scenarios |
| table_match_regex | string | no | .* | Regex to match table paths. Unmatched tables pass through unchanged |
| row_error_handle_way | enum | no | FAIL | How to handle row-level errors: `FAIL`, `SKIP`, or `ROUTE_TO_TABLE` |

### sql [string]

The SQL statement to execute using the Apache Calcite engine. The table name in `FROM` must match the `plugin_input` value.

```hocon
sql = "SELECT id, UPPER(name) AS name, age + 1 AS next_age FROM source_table WHERE age > 18"
```

### table_transform [list]

Per-table SQL overrides for multi-table CDC scenarios. Each entry specifies a `table_path` and a `sql` statement. Tables not listed fall back to the global `sql` (if their path matches `table_match_regex`) or pass through unchanged.

```hocon
table_transform = [
  {
    table_path = "db.users"
    sql = "SELECT id, name, UPPER(email) AS email FROM users"
  },
  {
    table_path = "db.orders"
    sql = "SELECT order_id, amount * 100 AS amount_cents FROM orders"
  }
]
```

### table_match_regex [string]

A regex pattern to filter which tables should be transformed. Only tables whose path matches this regex will have the global `sql` applied. Tables that do not match pass through unchanged. Default is `.*` (match all).

### row_error_handle_way [enum]

How to handle errors during SQL execution for a row:

- `FAIL` (default) -- fail the job immediately
- `SKIP` -- skip the problematic row and continue
- `ROUTE_TO_TABLE` -- route the error row to a separate error table

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details.

## Supported Types

Bidirectional type mapping between SeaTunnel and Calcite:

| SeaTunnel Type | Calcite Type | Notes |
|----------------|--------------|-------|
| BOOLEAN | BOOLEAN | |
| TINYINT | TINYINT | |
| SMALLINT | SMALLINT | |
| INT | INTEGER | |
| BIGINT | BIGINT | |
| FLOAT | REAL | |
| DOUBLE | DOUBLE | |
| DECIMAL(p,s) | DECIMAL(p,s) | Precision and scale preserved |
| STRING | VARCHAR | |
| BYTES | VARBINARY | |
| DATE | DATE | |
| TIME | TIME | |
| TIMESTAMP | TIMESTAMP | |
| TIMESTAMP_TZ | TIMESTAMP_WITH_LOCAL_TIME_ZONE | |
| NULL | NULL | |
| ARRAY | ARRAY | Element type recursively mapped |
| MAP | MAP | Key/value types recursively mapped |
| ROW | ROW (struct) | Field names and types preserved |
| BINARY_VECTOR | VARBINARY | Lossy: vector semantics not preserved in SQL |
| FLOAT_VECTOR | VARBINARY | Lossy: vector semantics not preserved in SQL |
| FLOAT16_VECTOR | VARBINARY | Lossy: vector semantics not preserved in SQL |
| BFLOAT16_VECTOR | VARBINARY | Lossy: vector semantics not preserved in SQL |
| SPARSE_FLOAT_VECTOR | VARBINARY | Lossy: vector semantics not preserved in SQL |

Calcite INTERVAL types (e.g., `INTERVAL YEAR`, `INTERVAL DAY TO SECOND`) are mapped to `BIGINT` on output.

## Built-in UDFs

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| MASK | `MASK(value, start, end, maskChar)` | STRING | Replaces characters in range `[start, end)` with `maskChar`. Returns original if range is invalid. Default mask char is `*` when null or empty |
| MASK_HASH | `MASK_HASH(value)` | STRING | Returns the SHA-256 hex hash (64 characters) of the input. Deterministic -- same input always produces same hash |
| DES_ENCRYPT | `DES_ENCRYPT(password, data)` | STRING | Encrypts `data` with DES (CBC/PKCS5Padding) using `password` (must be >= 8 chars). Returns Base64-encoded ciphertext |
| DES_DECRYPT | `DES_DECRYPT(password, data)` | STRING | Decrypts Base64-encoded `data` with the same password used for encryption |
| URL_ENCODE | `URL_ENCODE(value)` | STRING | URL-encodes the input string (UTF-8) |
| URL_DECODE | `URL_DECODE(value)` | STRING | URL-decodes the input string (UTF-8) |

All built-in UDFs return `NULL` when any required argument is `NULL`.

## Built-in SQL Functions

Calcite provides 200+ standard SQL functions. Below are commonly used categories:

### String Functions

`UPPER`, `LOWER`, `TRIM`, `CONCAT`, `SUBSTRING`, `REPLACE`, `CHAR_LENGTH`, `POSITION`, `OVERLAY`, `INITCAP`

### Math Functions

`ABS`, `MOD`, `POWER`, `SQRT`, `FLOOR`, `CEIL`, `ROUND`, `SIGN`, `LN`, `LOG10`, `EXP`

### Date/Time Functions

`CURRENT_DATE`, `CURRENT_TIMESTAMP`, `EXTRACT`, `TIMESTAMPADD`, `TIMESTAMPDIFF`, `YEAR`, `MONTH`, `DAYOFMONTH`

### JSON Functions

`JSON_VALUE(json, '$.path')`, `JSON_QUERY`, `JSON_EXISTS`

### Conditional Functions

`CASE WHEN ... THEN ... ELSE ... END`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`

### Comparison & Logical

`=`, `<>`, `<`, `>`, `IN (...)`, `BETWEEN ... AND ...`, `LIKE`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`

### Type Conversion

`CAST(expr AS type)`

For the full function reference, see the [Apache Calcite SQL Reference](https://calcite.apache.org/docs/reference.html).

> **Note:** Calcite Transform processes rows one at a time. Aggregate functions like `SUM`, `COUNT`, `AVG` are syntactically valid but operate on a single row, which is generally not useful. Use them only with window functions or in specific single-row contexts.

## Examples

### Basic SELECT + WHERE

The data read from source is a table like this:

| id | name | age |
|----|------|-----|
| 1 | Joy Ding | 20 |
| 2 | May Ding | 21 |
| 3 | Kin Dom | 24 |
| 4 | Joy Dom | 15 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, name, age FROM fake WHERE age >= 18"
  }
}
```

Then the data in result table `result` will be:

| id | name | age |
|----|------|-----|
| 1 | Joy Ding | 20 |
| 2 | May Ding | 21 |
| 3 | Kin Dom | 24 |

Row with `age = 15` is filtered out.

### String and Math Functions

Input:

| id | name | salary |
|----|------|--------|
| 1 | Joy Ding | 5000 |
| 2 | May Ding | 8000 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, UPPER(name) AS name_upper, CHAR_LENGTH(name) AS name_len, salary * 1.1 AS new_salary FROM fake"
  }
}
```

Output:

| id | name_upper | name_len | new_salary |
|----|------------|----------|------------|
| 1 | JOY DING | 8 | 5500.0 |
| 2 | MAY DING | 8 | 8800.0 |

### CASE WHEN

Input:

| id | name | age |
|----|------|-----|
| 1 | Alice | 8 |
| 2 | Bob | 15 |
| 3 | Carol | 30 |
| 4 | Dave | 70 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, name, CASE WHEN age < 13 THEN 'child' WHEN age < 18 THEN 'teen' WHEN age < 65 THEN 'adult' ELSE 'senior' END AS age_group FROM fake"
  }
}
```

Output:

| id | name | age_group |
|----|------|-----------|
| 1 | Alice | child |
| 2 | Bob | teen |
| 3 | Carol | adult |
| 4 | Dave | senior |

### JSON Extraction

Input:

| id | payload |
|----|---------|
| 1 | {"user": {"name": "Joy Ding", "email": "joy@example.com"}} |
| 2 | {"user": {"name": "May Ding", "email": "may@example.com"}} |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, JSON_VALUE(payload, '$.user.name') AS user_name, JSON_VALUE(payload, '$.user.email') AS email FROM fake"
  }
}
```

Output:

| id | user_name | email |
|----|-----------|-------|
| 1 | Joy Ding | joy@example.com |
| 2 | May Ding | may@example.com |

### Data Masking (MASK + MASK_HASH + DES)

Input:

| id | phone | secret |
|----|-------|--------|
| 1 | 13812345678 | seatunnel-password |
| 2 | 13987654321 | connector-api-key |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, MASK(phone, 3, 7, '*') AS masked_phone, MASK_HASH(phone) AS phone_hash, DES_ENCRYPT('12345678', secret) AS encrypted_secret FROM fake"
  }
}
```

Output:

| id | masked_phone | phone_hash | encrypted_secret |
|----|--------------|------------|------------------|
| 1 | 138\*\*\*\*5678 | a1b2c3...(64-char SHA-256 hex) | Base64-encoded ciphertext |
| 2 | 139\*\*\*\*4321 | d4e5f6...(64-char SHA-256 hex) | Base64-encoded ciphertext |

To decrypt later in the pipeline:

```hocon
transform {
  Calcite {
    plugin_input = "result"
    plugin_output = "decrypted"
    sql = "SELECT id, DES_DECRYPT('12345678', encrypted_secret) AS original_secret FROM result"
  }
}
```

### Multi-table CDC (table_transform)

```hocon
transform {
  Calcite {
    plugin_input = "cdc_source"
    plugin_output = "result"
    table_transform = [
      {
        table_path = "db.users"
        sql = "SELECT id, name, UPPER(email) AS email FROM users"
      },
      {
        table_path = "db.orders"
        sql = "SELECT order_id, amount * 100 AS amount_cents FROM orders"
      }
    ]
  }
}
```

Tables not listed in `table_transform` but matching `table_match_regex` (default `.*`) will have the global `sql` applied. Tables not matching any rule pass through unchanged.

### Error Handling (row_error_handle_way)

```hocon
transform {
  Calcite {
    plugin_input = "source_table"
    plugin_output = "result"
    sql = "SELECT id, CAST(age AS VARCHAR) AS age_str FROM source_table"
    row_error_handle_way = "SKIP"
  }
}
```

When a row causes a SQL execution error:

- `FAIL` -- the job fails immediately (default, recommended for data quality)
- `SKIP` -- the problematic row is silently dropped
- `ROUTE_TO_TABLE` -- the row is sent to a separate error table for later inspection

## Custom UDF Development Guide

Custom UDFs are added via the `CalciteUdf` SPI. Implement the interface, package as JAR, and place it in `${SEATUNNEL_HOME}/lib/`.

**Step 1.** Create a class that implements `CalciteUdf` and add a **public static `eval`** method:

```java
package com.example;

import org.apache.seatunnel.api.transform.CalciteUdf;
import com.google.auto.service.AutoService;
import java.util.Locale;

@AutoService(CalciteUdf.class)
public class MyUpperUdf implements CalciteUdf {

    @Override
    public String functionName() {
        return "MY_UPPER";
    }

    public static String eval(String input) {
        return input == null ? null : input.toUpperCase(Locale.ROOT);
    }
}
```

Key requirements:
- `eval` **must be `public static`** -- Calcite's code generation calls it directly without creating an instance
- The `eval` method signature defines the SQL function's input/output types (e.g., `String eval(String, int)` means the SQL function takes a VARCHAR and an INTEGER)
- `@AutoService(CalciteUdf.class)` generates the `META-INF/services` file for SPI discovery
- `functionName()` returns the SQL name (case-insensitive at query time)

**Step 2.** Add the `auto-service` dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.google.auto.service</groupId>
    <artifactId>auto-service</artifactId>
    <version>1.1.1</version>
    <scope>provided</scope>
</dependency>
```

**Step 3.** Build the JAR and place it in `${SEATUNNEL_HOME}/lib/`.

**Step 4.** Use in SQL:

```sql
SELECT MY_UPPER(name) AS upper_name FROM source_table
```

## Limitations

| Limitation | Detail |
|------------|--------|
| Single input table | Only one table is registered in the Calcite schema per transform. Multi-table `JOIN` is not supported |
| Row-at-a-time processing | Each row is processed independently. `GROUP BY` / `SUM()` / `COUNT()` operate on a single row and are generally not useful for batch aggregation |
| WHERE filtering | `WHERE` conditions that evaluate to `false` cause the row to be dropped (not passed through) |
| Table name matching | The `FROM` table name in SQL must exactly match the `plugin_input` value |
| Scalar UDFs only | Only scalar functions are supported. Table-valued functions and aggregate UDFs are not available |
| Vector type lossy | Vector types (BINARY_VECTOR, FLOAT_VECTOR, etc.) are mapped to VARBINARY, losing vector semantics |

## FAQ

**Q: What table name should I use in the SQL `FROM` clause?**

A: It must match the `plugin_input` value. For example, if `plugin_input = "fake"`, your SQL should be `SELECT ... FROM fake`.

**Q: Do I need to quote UDF function names?**

A: No. The Calcite engine is configured with case-insensitive identifier matching. `MASK(...)`, `mask(...)`, and `Mask(...)` all work.

**Q: Does it support JOIN?**

A: No. The Calcite Transform registers only one input table. For cross-table operations, chain multiple transforms or use a different approach.

**Q: Can I use GROUP BY or aggregate functions?**

A: They are syntactically valid but not practically useful. The engine processes one row at a time, so `SUM(amount)` returns the value of `amount` for that single row.

**Q: Must the `eval` method in a custom UDF be `static`?**

A: Yes. Calcite's code generation calls `eval` as a static method directly. An instance method would cause Calcite to create a new object for each call, bypassing any initialization done in `open()`.

**Q: How do I add a new UDF?**

A: Implement the `CalciteUdf` SPI with `@AutoService` and place the JAR in `${SEATUNNEL_HOME}/lib/`. See the [Custom UDF Development Guide](#custom-udf-development-guide) section.

**Q: How does it handle schema changes in CDC scenarios?**

A: When an `AlterTableEvent` is received (e.g., column added/dropped), the engine automatically rebuilds the SQL plan and re-infers the output schema.

## Job Config Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        phone = "string"
      }
    }
  }
}

transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, UPPER(name) AS name, age + 1 AS age, MASK(phone, 3, 7, '*') AS phone FROM fake WHERE age >= 0"
  }
}

sink {
  Console {
    plugin_input = "result"
  }
}
```

## Changelog

### next-release

- Add Calcite Transform plugin

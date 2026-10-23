# Calcite UDF

> User-Defined Functions for the Calcite Transform plugin

## Description

Use the `CalciteUdf` SPI to extend the [Calcite Transform](calcite.md) with custom scalar functions. Implementations are discovered at runtime via Java `ServiceLoader`.

## UDF API

```java
package org.apache.seatunnel.transform.calcite.udf;

public interface CalciteUdf extends AutoCloseable {

    /**
     * SQL function name used in queries, e.g. "MY_UPPER".
     * Case-insensitive at query time.
     */
    String functionName();

    /** Open UDF resources. Called once before first eval. */
    default void open() {}

    /** Release UDF resources. */
    @Override
    default void close() throws Exception {}
}
```

## UDF Implementation Example

### Step 1. Add Maven dependencies

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-transforms-v2</artifactId>
        <version>${seatunnel.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-api</artifactId>
        <version>${seatunnel.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>com.google.auto.service</groupId>
        <artifactId>auto-service</artifactId>
        <version>1.1.1</version>
        <scope>provided</scope>
    </dependency>
</dependencies>
```

### Step 2. Implement CalciteUdf

Create a class that implements `CalciteUdf` and add a **public static `eval`** method:

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
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

- `eval` **must be `public static`** -- Calcite's code generation calls it directly without creating an instance. An instance method would cause Calcite to create a new object for each call, bypassing any initialization done in `open()`.
- The `eval` method signature defines the SQL function's input/output types. For example, `String eval(String, int)` means the SQL function takes a VARCHAR and an INTEGER and returns a VARCHAR.
- `@AutoService(CalciteUdf.class)` generates the `META-INF/services` file for SPI discovery.
- `functionName()` returns the SQL function name. Function names are **case-insensitive** at query time -- `MY_UPPER(...)`, `my_upper(...)`, and `My_Upper(...)` all work.

### Step 3. Deploy

Build the JAR and place it in `${SEATUNNEL_HOME}/lib/`. If your UDF uses third-party libraries, include them in the same directory.

If you use cluster mode, you need to place the JAR on all nodes' `${SEATUNNEL_HOME}/lib/` and restart the cluster.

### Step 4. Use in SQL

```sql
SELECT MY_UPPER(name) AS upper_name FROM source_table
```

## Lifecycle UDF Example

If your UDF needs to initialize or release resources (e.g., database connections, caches), override `open()` and `close()`:

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import com.google.auto.service.AutoService;

@AutoService(CalciteUdf.class)
public class PrefixUdf implements CalciteUdf {

    private static volatile String prefix;

    @Override
    public String functionName() {
        return "WITH_PREFIX";
    }

    @Override
    public void open() {
        prefix = "HELLO";
    }

    public static String eval(String input) {
        if (input == null) return null;
        String p = prefix;
        return (p != null ? p : "") + ": " + input;
    }

    @Override
    public void close() {
        prefix = null;
    }
}
```

:::caution

Since `eval` must be static, shared state (like `prefix` above) must also be stored in a static field. Use `volatile` for simple references and proper synchronization for complex mutable state to ensure visibility across threads.

:::

## Context-aware UDF Example

If your UDF needs access to row-level metadata (e.g., RowKind for CDC, table path), use `CalciteUdfContext.current()` inside the static `eval` method:

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import org.apache.seatunnel.transform.calcite.udf.CalciteUdfContext;
import com.google.auto.service.AutoService;

@AutoService(CalciteUdf.class)
public class RowKindUdf implements CalciteUdf {

    @Override
    public String functionName() {
        return "ROW_KIND";
    }

    public static String eval(String input) {
        CalciteUdfContext ctx = CalciteUdfContext.current();
        if (ctx == null || input == null) return null;
        return ctx.getRowKind().shortString() + ":" + input;
    }
}
```

The `CalciteUdfContext` provides the following methods:

| Method | Return Type | Description |
|--------|-------------|-------------|
| `getRawTableId()` | String | Raw table identifier (e.g., `db.schema.table`) |
| `getDatabase()` | String | Parsed database name |
| `getSchema()` | String | Parsed schema name |
| `getTable()` | String | Parsed table name |
| `getRowKind()` | RowKind | Row change type: `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE` |

Usage:

```sql
SELECT ROW_KIND(name) AS kind_name FROM source_table
```

## Multi-parameter UDF Example

The `eval` method can accept multiple parameters of different types:

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import com.google.auto.service.AutoService;

@AutoService(CalciteUdf.class)
public class SubstringUdf implements CalciteUdf {

    @Override
    public String functionName() {
        return "MY_SUBSTR";
    }

    public static String eval(String input, int start, int length) {
        if (input == null) return null;
        int end = Math.min(start + length, input.length());
        return input.substring(Math.max(0, start), end);
    }
}
```

Usage:

```sql
SELECT MY_SUBSTR(name, 0, 3) AS short_name FROM source_table
```

## Complete Job Example

Input:

| id | name | age |
|----|------|-----|
| 1 | Joy Ding | 20 |
| 2 | May Ding | 21 |
| 3 | Kin Dom | 24 |
| 4 | Joy Dom | 22 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, MY_UPPER(name) AS name, age FROM fake"
  }
}
```

Output:

| id | name | age |
|----|------|-----|
| 1 | JOY DING | 20 |
| 2 | MAY DING | 21 |
| 3 | KIN DOM | 24 |
| 4 | JOY DOM | 22 |

## Type Mapping

The `eval` method's Java types map to SQL types as follows:

| Java Type | SQL Type |
|-----------|----------|
| `String` | VARCHAR |
| `int` / `Integer` | INTEGER |
| `long` / `Long` | BIGINT |
| `float` / `Float` | REAL |
| `double` / `Double` | DOUBLE |
| `boolean` / `Boolean` | BOOLEAN |
| `java.math.BigDecimal` | DECIMAL |
| `byte[]` | VARBINARY |

## Changelog

### next-release

- Add Calcite UDF documentation

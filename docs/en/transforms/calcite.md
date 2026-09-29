# Calcite

> Calcite SQL transform plugin

## Description

SQL transform plugin powered by [Apache Calcite](https://calcite.apache.org/). Use standard SQL to transform data rows. The SQL plan is compiled once at job startup and applied to each row at runtime.

:::tip

- Each row is processed independently -- `JOIN` and cross-row aggregation (`GROUP BY`, `SUM`, `COUNT`) are **not** supported.
- Vector types (FLOAT_VECTOR, BINARY_VECTOR, etc.) are mapped to VARBINARY internally. Use the built-in vector UDFs (e.g., `COSINE_DISTANCE`, `VECTOR_REDUCE`) for vector operations.

:::

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| sql | string | yes | - | SQL statement to execute |
| table_transform | list | no | [] | Per-table SQL overrides for multi-table CDC scenarios |
| table_match_regex | string | no | .* | Regex to match table paths. Unmatched tables pass through unchanged |
| row_error_handle_way | enum | no | FAIL | How to handle row-level errors: `FAIL`, `SKIP`, or `ROUTE_TO_TABLE` |

### sql [string]

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

## Built-in UDFs

All built-in UDFs return `NULL` when any required argument is `NULL`.
Function identifiers are case-insensitive. For example, `MASK(...)`, `mask(...)`, and `Mask(...)` are equivalent.

### Data Masking Functions

#### MASK

```MASK(value, start, end, maskChar) -> STRING```

Replaces characters in range `[start, end)` with `maskChar`. Returns the original string if the range is invalid. Default mask char is `*` when null or empty.

Example:

```sql
SELECT MASK(phone, 3, 7, '*') AS masked_phone FROM t
```

#### MASK_HASH

```MASK_HASH(value) -> STRING```

Returns the SHA-256 hex hash (64 characters) of the input. Deterministic -- same input always produces the same hash.

Example:

```sql
SELECT MASK_HASH(phone) AS phone_hash FROM t
```

#### DES_ENCRYPT

```DES_ENCRYPT(password, data) -> STRING```

Encrypts `data` with DES (CBC/PKCS5Padding) using `password` (must be >= 8 chars). Returns Base64-encoded ciphertext.

Example:

```sql
SELECT DES_ENCRYPT('12345678', secret) AS encrypted FROM t
```

#### DES_DECRYPT

```DES_DECRYPT(password, data) -> STRING```

Decrypts Base64-encoded `data` with the same password used for encryption.

Example:

```sql
SELECT DES_DECRYPT('12345678', encrypted_secret) AS original FROM t
```

### Vector Functions

#### COSINE_DISTANCE

```COSINE_DISTANCE(vector1, vector2) -> DOUBLE```

Returns a DOUBLE value between 0 and 1: 0 means identical vectors (completely similar), 1 means orthogonal vectors (completely dissimilar).

Example:

```sql
SELECT COSINE_DISTANCE(vec1, vec2) AS distance FROM t
```

#### L1_DISTANCE

```L1_DISTANCE(vector1, vector2) -> DOUBLE```

Calculates the Manhattan (L1) distance between two vectors.

Example:

```sql
SELECT L1_DISTANCE(vec1, vec2) AS dist FROM t
```

#### L2_DISTANCE

```L2_DISTANCE(vector1, vector2) -> DOUBLE```

Calculates the Euclidean (L2) distance between two vectors.

Example:

```sql
SELECT L2_DISTANCE(vec1, vec2) AS dist FROM t
```

#### VECTOR_DIMS

```VECTOR_DIMS(vector) -> INT```

Returns an INT value representing the number of dimensions (elements) in the vector.

Example:

```sql
SELECT VECTOR_DIMS(embedding) AS dims FROM t
```

#### VECTOR_NORM

```VECTOR_NORM(vector) -> DOUBLE```

Calculates the L2 norm (Euclidean norm) of a vector, which represents the length or magnitude of the vector.

Example:

```sql
SELECT VECTOR_NORM(embedding) AS norm FROM t
```

#### INNER_PRODUCT

```INNER_PRODUCT(vector1, vector2) -> DOUBLE```

Calculates the inner product (dot product) of two vectors, which is used to measure the similarity and projection between the vectors.

Example:

```sql
SELECT INNER_PRODUCT(vec1, vec2) AS ip FROM t
```

#### VECTOR_REDUCE

```VECTOR_REDUCE(vector_field, target_dimension, method)```

Generic vector dimension reduction function that supports multiple reduction methods.

**Parameters:**
- `vector_field`: The vector field to reduce (VECTOR type)
- `target_dimension`: The target dimension (INTEGER, must be smaller than source dimension)
- `method`: The reduction method (STRING):
  - **'TRUNCATE'**: Truncates the vector by keeping only the first N elements. Simplest and fastest, but may lose information in truncated dimensions.
  - **'RANDOM_PROJECTION'**: Uses Gaussian random projection. Preserves relative distances between vectors following the Johnson-Lindenstrauss lemma.
  - **'SPARSE_RANDOM_PROJECTION'**: Uses sparse random projection where matrix elements are mostly zero. More computationally efficient than regular random projection.

**Returns:** VARBINARY -- the reduced vector

**Example:**

```sql
SELECT id, VECTOR_REDUCE(embedding, 256, 'TRUNCATE') AS reduced FROM t
SELECT id, VECTOR_REDUCE(embedding, 128, 'RANDOM_PROJECTION') AS reduced FROM t
SELECT id, VECTOR_REDUCE(embedding, 64, 'SPARSE_RANDOM_PROJECTION') AS reduced FROM t
```

#### VECTOR_NORMALIZE

```VECTOR_NORMALIZE(vector_field)```

Normalizes a vector to unit length (magnitude = 1). Useful for computing cosine similarity.

**Parameters:**
- `vector_field`: The vector field to normalize (VECTOR type)

**Returns:** VARBINARY -- the normalized vector

**Example:**

```sql
SELECT id, VECTOR_NORMALIZE(embedding) AS unit_vec FROM t
```

In addition to the UDFs listed above, all standard SQL functions provided by Apache Calcite are available (string, math, date/time, JSON, conditional, etc.). For the full function reference, see the [Apache Calcite SQL Reference](https://calcite.apache.org/docs/reference.html).

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

### CASE WHEN Conditional

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

### Vector Operations

Use built-in vector UDFs to compute distances, reduce dimensions, or normalize vectors in a data pipeline (e.g., between Milvus/Qdrant source and sink).

```hocon
transform {
  Calcite {
    plugin_input = "vector_source"
    plugin_output = "result"
    sql = "SELECT id, COSINE_DISTANCE(query_vec, doc_vec) AS distance, VECTOR_DIMS(doc_vec) AS dims, VECTOR_REDUCE(doc_vec, 128, 'TRUNCATE') AS reduced_vec FROM vector_source"
  }
}
```

Given two FLOAT_VECTOR columns `query_vec` and `doc_vec`, this computes the cosine distance, extracts dimensions, and reduces `doc_vec` from its original dimension to 128.

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

## Custom UDF

Custom scalar functions can be added via the `CalciteUdf` SPI. For the full development guide, API reference, examples, and type mapping, see [Calcite UDF](calcite-udf.md).

## Limitations

| Limitation | Detail |
|------------|--------|
| Single input table | Only one table is registered in the Calcite schema per transform. Multi-table `JOIN` is not supported |
| Row-at-a-time processing | Each row is processed independently. `GROUP BY` / `SUM()` / `COUNT()` operate on a single row and are generally not useful for batch aggregation |
| WHERE filtering | `WHERE` conditions that evaluate to `false` cause the row to be dropped (not passed through) |
| Table name matching | The `FROM` table name in SQL must exactly match the `plugin_input` value |
| Scalar UDFs only | Only scalar functions are supported. Table-valued functions and aggregate UDFs are not available |
| Vector type mapping | Vector types are mapped to VARBINARY internally. Use built-in vector UDFs (COSINE_DISTANCE, L1_DISTANCE, etc.) for vector operations |

:::tip CDC schema changes
When an `AlterTableEvent` is received (for example, add/drop columns), the engine automatically rebuilds the SQL plan and re-infers the output schema. No manual intervention is needed.
:::

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

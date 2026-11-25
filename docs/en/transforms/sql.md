# SQL

> SQL transform plugin

## Description

Use SQL to transform given input row.

SQL transform use memory SQL engine, we can via SQL functions and ability of SQL engine to implement the transform task.

## Options

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| plugin_input | string | no       | -             |
| plugin_output | string | no       | -             |
| query             | string | yes      | -             |
| engine            | string | no       | ZETA          |
| where_rejected_update_as_delete | boolean | no | false |

### plugin_input [string]

The source table name, the query SQL table name must match this field. If not configured, the input catalog table name is used.

### query [string]

The query SQL, it's a simple SQL supported base function and criteria filter operation. But the complex SQL unsupported yet, include: multi source table/rows JOIN and AGGREGATE operation and the like.

the query expression can be `select [table_name.]column_a` to query the column that named `column_a`. and the table name is optional.  
or `select c_row.c_inner_row.column_b` to query the inline struct column that named `column_b` within `c_row` column and `c_inner_row` column. **In this query expression, can't have table name.**

### engine [string]

The SQL engine used by this transform. Supported values are `ZETA` and `INTERNAL`. If this option is not configured, `ZETA` is used.

### where_rejected_update_as_delete [boolean]

Whether an `UPDATE_AFTER` row that the `WHERE` clause rejects is emitted as a `DELETE` of its projected row. When `false` (the default), it is dropped like any other row that does not match. Enable it only when the query selects a key that the update does not change and the sink deletes rows by that key; see [WHERE on a CDC stream](#where-on-a-cdc-stream). In `table_transform`, set it in each entry that needs it: an entry does not inherit it from the transform level.

## Numeric comparisons

Comparisons preserve numeric precision when both evaluated operands are integral
types (`TINYINT`, `SMALLINT`, `INT`, `BIGINT`) or `DECIMAL`. This applies to `=`,
`!=`, `<>`, `<`, `<=`, `>` and `>=`, numeric `IN` and `NOT IN` membership checks,
and comparisons in searched and simple `CASE` expressions.
For example, a `BIGINT` value of `9007199254740993` does not match
`WHERE id = 9007199254740992`.

If either operand is `FLOAT` or `DOUBLE`, comparisons continue to use
floating-point conversion, retaining the existing behavior for NaN, infinity
and signed zero. Numeric literals containing a decimal point are evaluated as
`DOUBLE`, not `DECIMAL`: comparing a `DECIMAL` column to
`123456789012345678.98` can still match `123456789012345678.99`. Integer literals
evaluated as integral values, such as `-9007199254740993`, use the exact path
when the other operand is also exact. Unary minus preserves `DECIMAL` precision,
so both `-decimal_col` and `-CAST('...' AS DECIMAL(p, s))` retain exact decimal
comparison semantics when the other operand is also exact.

For an exact decimal constant, use a quoted string with sufficient scale, for
example `amount = CAST('123456789012345678.99' AS DECIMAL(38, 2))`.
For a negative constant, either `CAST('-123456789012345678.99' AS DECIMAL(38, 2))`
or `-CAST('123456789012345678.99' AS DECIMAL(38, 2))` preserves decimal precision.
Existing CAST rounding is unchanged: converting a string or `DECIMAL` to
`DECIMAL(p, s)` uses `CEILING` (toward positive infinity) if scale is reduced,
so `CAST('1.231' AS DECIMAL(10, 2))` is `1.24` and
`CAST('-1.239' AS DECIMAL(10, 2))` is `-1.23`. Negating after CAST instead gives
`-1.24` for `-CAST('1.239' AS DECIMAL(10, 2))`: reducing scale rounds before
negation, so the two forms are not interchangeable in that case. Converting
`FLOAT` or `DOUBLE` uses `HALF_UP`; casting a fractional numeric literal cannot recover precision
already lost during its evaluation as `DOUBLE`.

The exact-or-floating choice is made for each operand pair, including each
element of an `IN` list. For `id = 9007199254740993`,
`id IN (9007199254740992)` is false, but
`id IN (9007199254740992, 9007199254740992.0)` is true because the second
comparison uses `DOUBLE`. Equality is therefore not necessarily transitive
across mixed exact and floating types; use exact operands consistently when
precision matters. These rules apply to both `ZETA` and `INTERNAL`.

## WHERE on a CDC stream

The `WHERE` clause judges each row on its own and keeps its row kind. On a CDC stream an update arrives as an `UPDATE_BEFORE` row with the old values and an `UPDATE_AFTER` row with the new values. When an update moves a row out of the condition, the `UPDATE_BEFORE` matches and is kept, and the `UPDATE_AFTER` is dropped. A sink that writes `UPDATE_AFTER` as an upsert and skips `UPDATE_BEFORE`, such as Jdbc with a primary key, then keeps the old row.

With `where_rejected_update_as_delete = true`, the transform emits:

| Row kind | Matches the `WHERE` clause | Output |
|----------|----------------------------|--------|
| any | yes | the projected row, with the same row kind |
| `UPDATE_AFTER` | no | the projected row, as a `DELETE` |
| `INSERT`, `UPDATE_BEFORE`, `DELETE` | no | nothing |

An update that moves a row out of the condition then reaches the sink as a `DELETE`, and a sink that writes rows by key removes the row. With the default `false`, every row that does not match is dropped, as in earlier versions.

```hocon
transform {
  Sql {
    plugin_input = "cdc"
    plugin_output = "active_users"
    query = "select id, name, status from dual where status = 'active'"
    where_rejected_update_as_delete = true
  }
}
```

Enable it only when both of these hold:

- **A stable key:** the query selects the key columns that the sink deletes by, and the update does not change them. The `DELETE` is built from the new values only, so it finds the row only by a key that is the same before and after the update. The key can come from the primary key of the source schema or from the sink configuration, such as `primary_keys` on Jdbc.
- **A sink that deletes:** the sink deletes rows by that key. A sink without a key appends every row it receives; for example, a Jdbc sink without `primary_keys` and without a primary key in the schema writes the `DELETE` as a new row.

Limits of the projection:

- If the query does not select the key, for example `select name, status from dual where status = 'active'` for a sink keyed by `id`, the output schema has no primary key and the `DELETE` carries only the selected columns, so the sink cannot find the row. A Jdbc sink whose `primary_keys` names a column that the query does not select fails at start, with or without this option.
- A `DELETE` is also sent when the old values did not match either. A keyed sink then deletes a key it does not hold, which changes no data, but the sink needs the permission to delete. The Kudu sink fails on such a delete unless `ignore_not_found = true`.
- With `LATERAL VIEW`, every row produced from a rejected `UPDATE_AFTER` is a `DELETE`.
- If the projection of a rejected `UPDATE_AFTER` fails, for example `select 10 / x as r from dual where x <> 0` with `x = 0`, the row is dropped and a warning is logged.
- A target table loaded before the option was enabled can still hold rows that no longer match; reload it from the source to remove them.

## Example

The data read from source is a table like this:

| id |   name   | age |
|----|----------|-----|
| 1  | Joy Ding | 20  |
| 2  | May Ding | 21  |
| 3  | Kin Dom  | 24  |
| 4  | Joy Dom  | 22  |

We use SQL query to transform the source data like this:

```
transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from dual where id>0"
  }
}
```

Then the data in result table `fake1` will update to

| id |   name    | age |
|----|-----------|-----|
| 1  | Joy Ding_ | 21  |
| 2  | May Ding_ | 22  |
| 3  | Kin Dom_  | 25  |
| 4  | Joy Dom_  | 23  |

### Struct query

if your upstream data schema is like this:

```hocon
source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    string.template = ["innerQuery"]
    schema = {
      fields {
        name = "string"
        c_date = "date"
        c_row = {
          c_inner_row = {
            c_inner_int = "int"
            c_inner_string = "string"
            c_inner_timestamp = "timestamp"
            c_map_1 = "map<string, string>"
            c_map_2 = "map<string, map<string,string>>"
          }
          c_string = "string"
        }
      }
    }
  }
}
```

Those query all are valid:

```sql
select 
name,
c_date,
c_row,
c_row.c_inner_row,
c_row.c_string,
c_row.c_inner_row.c_inner_int,
c_row.c_inner_row.c_inner_string,
c_row.c_inner_row.c_inner_timestamp,
c_row.c_inner_row.c_map_1,
c_row.c_inner_row.c_map_1.some_key
```

But this query are not valid:

```sql
select 
c_row.c_inner_row.c_map_2.some_key.inner_map_key
```

The map must be the latest struct, can't query the nesting map.

## Job Config Example

```
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
      }
    }
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from dual where id>0"
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## Collection expressions

ARRAY and MAP arguments may use unary `+`/`-` and parentheses. For example, `ARRAY(-1, +2, (3))` produces `[-1, 2, 3]`, and `MAP('negative', (-1))` produces a map with value `-1`.

Signed collection expressions retain the numeric element type and propagate NULL operands. TINYINT arrays now use `ARRAY<TINYINT>` rather than the previous incorrect `ARRAY<STRING>` schema.

## Changelog

- Support struct query

### new version

- Add SQL Transform Connector

# SQL

> SQL transform plugin

## Description

Use SQL to transform given input row.

SQL transform use memory SQL engine, we can via SQL functions and ability of SQL engine to implement the transform task.

## Options

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| plugin_input | string | yes      | -             |
| plugin_output | string | yes      | -             |
| query             | string | yes      | -             |
| engine            | string | no       | ZETA          |

### plugin_input [string]

The source table name, the query SQL table name must match this field.

### query [string]

The query SQL, it's a simple SQL supported base function and criteria filter operation. But the complex SQL unsupported yet, include: multi source table/rows JOIN and AGGREGATE operation and the like.

the query expression can be `select [table_name.]column_a` to query the column that named `column_a`. and the table name is optional.  
or `select c_row.c_inner_row.column_b` to query the inline struct column that named `column_b` within `c_row` column and `c_inner_row` column. **In this query expression, can't have table name.**

### engine [string]

The SQL engine used by this transform. Supported values are `ZETA` and `INTERNAL`. If this option is not configured, `ZETA` is used.

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

```hacon
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

## Schema evolution (DDL)

When the upstream source emits schema change events (for example `MySQL-CDC` with `schema-changes.enabled = true`), the SQL transform translates every column-level change into a change of its own output instead of forwarding the upstream event. The event that reaches the sink therefore always describes the columns the sink actually receives. Only the Zeta engine delivers schema change events to transforms.

```hocon
transform {
  Sql {
    plugin_input = "products_cdc"
    plugin_output = "products_sql"
    query = "select id, name, weight, weight * 2 as double_weight from products"
  }
}
```

With this query an upstream `ADD COLUMN description` is absorbed, an upstream `MODIFY COLUMN name` reaches the sink as a modify of `name`, and an upstream `MODIFY COLUMN weight DECIMAL(12,3)` reaches the sink as a modify of `weight` and, because the derived type of `weight * 2` changes from DOUBLE to DECIMAL(12,3), as a modify of `double_weight`. A `MODIFY COLUMN weight DOUBLE` on a FLOAT column reaches the sink as a modify of `weight` only, because `weight * 2` is already derived as DOUBLE.

### Rules

| Upstream change | Query shape | Effect on the output and on the sink |
|-----------------|-------------|--------------------------------------|
| `ADD COLUMN` | `select *` | The column is added at the same position. |
| `ADD COLUMN` | `select *, expr AS x` | The column is added after the last star column, before the expression columns. |
| `ADD COLUMN` | Query without `*` | Absorbed; nothing reaches the sink. |
| `DROP COLUMN` | `select *` | The column is dropped. |
| `DROP COLUMN` | Column not referenced by the query | Absorbed. |
| `DROP COLUMN` or rename of a column the query references | Any | The job fails with `TRANSFORM_COMMON-09`, unless the same statement re-creates a column with that name (see lineage below). |
| Rename (`CHANGE COLUMN`) | `select *` | The column is renamed. |
| `MODIFY COLUMN` | `select *`, `select c`, `select c AS d` | The output column is modified with the new source type. |
| `MODIFY COLUMN` | Expression such as `weight * 2 AS double_weight` | The expression column is modified only when its derived type changes; its sink type is converted from the SeaTunnel type, not copied from the source type. |
| `MODIFY COLUMN` | `cast(c AS ...)` | Absorbed; the cast fixes the output type. |
| Column comment | `select *`, `select c` | The comment of the output column is updated. |
| Table comment | Any | Forwarded unchanged. |

Lineage. Changes are attributed to output columns by the identity of the physical source column. One statement that renames a column and adds a new column with the old name (`ALTER TABLE t CHANGE a b INT, ADD COLUMN a INT`) renames the sink column and adds a new one; one statement that drops and re-creates a column (`ALTER TABLE t DROP COLUMN a, ADD COLUMN a BIGINT`) drops and re-creates the sink column, also when the query references `a` directly. A rename that is reverted in the same statement changes nothing.

### Limits

- Dropping or renaming a column that belongs to the primary key, a constraint key or the partition keys of the output fails the job with `TRANSFORM_COMMON-09`, because sinks cannot update keys through schema change events. Modifying such a column is supported.
- Renames that form a cycle in one statement (`a -> b, b -> a`) fail the job.
- A change that would produce two output columns with the same name (for example `select *, id AS age` when the source adds `age`) fails the job.
- Type changes are checked at the event for the select list and for ordering comparisons in `WHERE` such as `c > 0`. A type change that only surfaces inside a function call, a UDF or a lateral view argument is not detected at the event and fails at row time, as before.
- Transforms placed before the SQL transform must describe their own output in the events they forward. If the upstream produced schema does not match the event, the job fails with `TRANSFORM_COMMON-09` instead of writing misaligned rows.

## Changelog

- Support schema evolution (DDL) events
- Support struct query

### new version

- Add SQL Transform Connector

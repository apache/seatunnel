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

### plugin_input [string]

The source table name, the query SQL table name must match this field.

### query [string]

The query SQL, it's a simple SQL supported base function and criteria filter operation. But the complex SQL unsupported yet, include: multi source table/rows JOIN and AGGREGATE operation and the like.

the query expression can be `select [table_name.]column_a` to query the column that named `column_a`. and the table name is optional.  
or `select c_row.c_inner_row.column_b` to query the inline struct column that named `column_b` within `c_row` column and `c_inner_row` column. **In this query expression, can't have table name.**

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

## Changelog

- Support struct query

### new version

- Add SQL Transform Connector


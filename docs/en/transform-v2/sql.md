# SQL

> SQL transform plugin

## Description

Use SQL to transform given input row.

SQL transform use memory SQL engine, we can via SQL functions and ability of SQL engine to implement the transform task.

## Options

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| source_table_name | string | yes      | -             |
| result_table_name | string | yes      | -             |
| query             | string | yes      | -             |

### source_table_name [string]

The source table name, the query SQL table name must match this field.

### query [string]

The query SQL, it's a simple SQL supported base function and criteria filter operation. But the complex SQL unsupported yet, include: multi source table/rows JOIN and AGGREGATE operation and the like.

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
    source_table_name = "fake"
    result_table_name = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from fake where id>0"
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

## Job Config Example

```
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
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
    source_table_name = "fake"
    result_table_name = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from fake where id>0"
  }
}

sink {
  Console {
    source_table_name = "fake1"
  }
}
```

## Changelog

### new version

- Add SQL Transform Connector


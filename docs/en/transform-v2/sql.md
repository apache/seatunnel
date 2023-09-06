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

## When Complex Types

Assuming this JSON from Kafka

```
{
  "c_string": "cs",
  "c_array": ["ca1", "ca2", "ca3"],
  "c_map": {
    "cmk1": "cmv1",
    "cmk2": "cmv2",
    "cmk3": "cmv3",
    "cmk4": "cmv4",
    "cmk5": "cmv5"
  },
  "c_row": {
    "c_int": 1,
    "c_string": "cr1",
    "next_row": {
      "c_int": 2,
      "c_string": "cr2"
    }
  }
}
```

We use SQL query to transform the source data like this:

```
transform {
  Sql {
    source_table_name = "t1"
    result_table_name = "t2"
    query = "select c_string as c1,c_array[0] as c2,c_map['cmk1'] as c3,c_row.next_row.c_string as c4 from t1"
  }
}
```

Then the data in result table `t2` will update to

| c1 | c2  |  c3  | c4  |
|----|-----|------|-----|
| cs | ca1 | cmv1 | cr2 |

Use FakeSource replace KafkaSource for this Example with Fast Test:

```
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
    FakeSource {
      result_table_name="t1"
      schema = {
        fields {
          c_string = "string"
          c_array = "array<string>"
          c_map = "map<string,string>"
          c_row {
            c_int = "int"
            c_string = "string"
            next_row {
              c_int = "int"
              c_string = "string"
            }
          }
        }
      }
      rows = [
        {
          kind = INSERT
          fields = ["cs",["ca1","ca2","ca3"],{"cmk1":"cmv1","cmk2":"cmv2","cmk3":"cmv3","cmk4":"cmv4","cmk5":"cmv5"},[1,"cr1",[2,"cr2"]]]
        }
      ]
    }
}

transform {
  Sql {
    source_table_name = "t1"
    result_table_name = "t2"
    query = "select c_string as c1,c_array[0] as c2,c_map['cmk1'] as c3,c_row.next_row.c_string as c4 from t1"
  }
}

sink {
  Console {
    source_table_name = "t2"
  }
}
```

## Changelog

### new version

- Add SQL Transform Connector


# Apache Pegasus

> Apache Pegasus sink connector.

## Description

Used to write data to Apache Pegasus.

## Key features

## Options

|    name     |  type  | required | default value |
|-------------|--------|----------|---------------|
| meta_server | string | true     | -             |
| table       | string | true     | -             |

## Example

```
Pegasus {
  source_table_name = "fake"
  meta_server = localhost:8170
  table = t1
}
```


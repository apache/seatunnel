# Pegasus

> Pegasus source connector

## Description

Used to read data from Pegasus.

## Options

|               name               |  type   | required |  default value   |
|----------------------------------|---------|----------|------------------|
| meta_server                      | string  | yes      | -                |
| table                            | string  | yes      | -                |
| read_mode                        | enum    | yes      | unorderedScanner |
| scan_option.timeoutMillis        | Int     | no       | -                |
| scan_option.batchSize            | Int     | no       | -                |
| scan_option.startInclusive       | boolean | no       | -                |
| scan_option.stopInclusive        | boolean | no       | -                |
| scan_option.hashKeyFilterType    | enum    | no       | -                |
| scan_option.hashKeyFilterPattern | string  | no       | -                |
| scan_option.sortKeyFilterType    | enum    | no       | -                |
| scan_option.sortKeyFilterPattern | string  | no       | -                |
| scan_option.noValue              | boolean | no       | -                |

## Example

simple:

```hocon
Pegasus {
  parallelism = 1
  result_table_name = "fake"
  meta_server = "localhost:8170"
  table = "t1"
  read_mode="unorderedScanner"
}
```


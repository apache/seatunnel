import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Hive

## Description

Write data to Hive.

Used to write data to file. Supports Batch and Streaming mode.

## Options

| name                              | type   | required | default value                                                 |
| --------------------------------- | ------ | -------- | ------------------------------------------------------------- |
| hive_table_name                   | string | yes      | -                                                             |
| hive_metastore_uris               | string | yes      | -                                                             |
| partition_by                      | array  | no       | -                                                             |
| sink_columns                      | array  | no       | When this parameter is empty, all fields are sink columns     |
| is_enable_transaction             | boolean| no       | true                                                          |
| save_mode                         | string | no       | "append"                                                      |

### hive_table_name [string]

Target Hive table name eg: db1.table1

### hive_metastore_uris [string]

Hive metastore uris

### partition_by [array]

Partition data based on selected fields

### sink_columns [array]

Which columns need be write to hive, default value is all of the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Only support `true` now.

### save_mode [string]

Storage mode, currently supports `overwrite` , `append`

Streaming Job not support `overwrite`.

## Example

```bash

Hive {
    hive_table_name="db1.table1"
    hive_metastore_uris="thrift://localhost:9083"
    partition_by=["age"]
    sink_columns=["name","age"]
    is_enable_transaction=true
    save_mode="append"
}

```

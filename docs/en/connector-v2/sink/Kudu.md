# Kudu

> Kudu sink connector

## Description

Write data to Kudu.

 The tested kudu version is 1.11.1.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name                     | type    | required | default value |
|--------------------------|---------|----------|---------------|
| kudu_master             | string  | yes      | -             |
| kudu_table               | string  | yes      | -             |
| save_mode               | string  | yes      | -             |

### kudu_master [string]

`kudu_master`  The address of kudu master,such as '192.168.88.110:7051'.

### kudu_table [string]

`kudu_table` The name of kudu table..

### save_mode [string]

Storage mode, we need support `overwrite` and `append`. `append` is now supported.

## Example

```bash

 kuduSink {
      kudu_master = "192.168.88.110:7051"
      kudu_table = "studentlyhresultflink"
      save_mode="append"
   }

```

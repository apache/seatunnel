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
| kudu_master              | string  | yes      | -             |
| kudu_table               | string  | yes      | -             |
| save_mode                | string  | yes      | -             |
| common-options           |         | no       | -             |

### kudu_master [string]

`kudu_master`  The address of kudu master,such as '192.168.88.110:7051'.

### kudu_table [string]

`kudu_table` The name of kudu table..

### save_mode [string]

Storage mode, we need support `overwrite` and `append`. `append` is now supported.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

```bash

 kuduSink {
      kudu_master = "192.168.88.110:7051"
      kudu_table = "studentlyhresultflink"
      save_mode="append"
   }

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Kudu Sink Connector

### 2.3.0-beta 2022-10-20
- [Improve] Kudu Sink Connector Support to upsert row ([2881](https://github.com/apache/incubator-seatunnel/pull/2881))

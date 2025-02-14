# SensorsData

> SensorsData sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Description

A sink plugin which use SensorsData SDK send data records.

## Options

|        name        |  type   | required | default value |
|--------------------|---------|----------|---------------|
| server_url         | string  | yes      | -             |
| bulk_size          | int     | no       | 50            |
| max_cache_row_size | int     | no       | 0             |
| consumer           | string  | no       | -             |
| entity_name        | string  | yes      | users         |
| record_type        | string  | yes      | users         |
| schema             | string  | yes      | users         |
| distinct_id_column | string  | yes      | -             |
| identity_fields    | array   | yes      | -             |
| property_fields    | array   | yes      | -             |
| event_name         | string  | yes      | -             |
| time_column        | string  | yes      | -             |
| time_free          | boolean | no       | false         |
| detail_id_column   | string  | no       | -             |
| item_id_column     | string  | no       | -             |
| item_type_column   | string  | no       | -             |
| skip_error_record  | boolean | no       | false         |
| common-options     |         | no       | -             |

### server_url [string]

SensorsData data sink address, the format is https://${host}:8106/sa?project=${project}

### bulk_size [int]

Threshold for the triggering flush operation in SensorsData SDK. When the memory cache queue reaches this value, the
data in the cache will be sended. The default value is 50.

### max_cache_row_size[int]

Maximum cache refresh size for SensorsData SDK. If it exceeds this value, the flush operation will be triggered
immediately. The default value is 0, which depends on bulkSize.

### consumer[string]

When consumer is set to "console", the data will be output to console instead of send to the server.

### entity_name[string]

The entity name of the SensorsData entity data model to receive the data records.

### record_type[string]

The record type of the SensorsData entity data model.

### schema[string]

The schema name of the SensorsData entity data model.

### distinct_id_column[string]

The distinct id column of the user entity.

### identity_fields[array]

The identity fields of the user entity.

### property_fields[array]

The property fields of the data record.

### event_name[string]

Currently, two formats are supported:

1. Fill in the name of the event record.
2. Use value of a field from upstream data as the event name, the format is ${your field name}, where event name is the
   value of the columns of the upstream data.

For example, Upstream data is the following:

|   name   | prop1 |     prop2     |
|----------|-------|---------------|
| Purchase | 16    | data-example1 |
| Order    | 23    | data-example2 |

If ${name} is set as the event name, the event name of the first row is "Purchase", and the event name of the second row
is "Order".

### time_column[string]

The time column of the event record.

### time_free[boolean]

Enable historical data mode.

### detail_id_column[string]

The detail id column of the user entity.

### item_id_column[string]

The item id column of the item entity.

### item_type_column[string]

The item type column of the item entity.

### skip_error_record[string]

Whether ignore the error in translating the data record.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    time_free = true

    record_type = events
    schema = events
    event_name = "$AppStart"
    time_column = col_date
    distinct_id_column = col_id
    identity_fields = [
      { source = col_id, target = "$identity_login_id" }
      { source = col_id, target = "$identity_distinct_id" }
    ]
    property_fields = [
      { target = prop1, source = col1, type = INT }
      { target = prop2, source = col2, type = BIGINT }
      { target = prop3, source = col3, type = STRING }
      { target = prop4, source = col4, type = BOOLEAN }
    ]
    skip_error_record = true
  }
}
```

## Changelog

### 2.3.8-beta 2024-11-13

- Add SensorsData Sink Connector


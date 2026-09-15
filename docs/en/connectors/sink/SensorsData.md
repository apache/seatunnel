import ChangeLog from '../changelog/connector-sensorsdata.md';

# SensorsData

> SensorsData sink connector

## Description

The SensorsData sink uses the SensorsData SDK to send SeaTunnel rows as SensorsData records. It
supports user events, user detail records, and item records. Use `consumer = "console"` when you
want to print the converted records locally instead of sending them to the SensorsData server.

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name                      | Type    | Required    | Default | Description |
|---------------------------|---------|-------------|---------|-------------|
| server_url                | string  | Yes         | -       | SensorsData data receiving address, for example `https://host:8106/sa?project=default`. |
| bulk_size                 | int     | No          | 50      | Flush threshold used by the SensorsData SDK cache. |
| max_cache_row_size        | int     | No          | 0       | Maximum cache size before an immediate flush. `0` means it follows `bulk_size`. |
| consumer                  | string  | No          | batch   | Consumer type. Use `batch` to send to SensorsData, or `console` to print records for local checks. |
| entity_name               | string  | Yes         | users   | Entity name. Supported values are `users` and `items`. |
| record_type               | string  | Yes         | users   | Record type. Common values are `events`, `details`, and `items`. |
| schema                    | string  | Conditional | users   | Schema name. Required for user records (`entity_name = "users"`). |
| distinct_id_column        | string  | Conditional | -       | Input column used as the SensorsData distinct ID. Required for user records. |
| identity_fields           | array   | Conditional | -       | Identity mappings. Required for user records. |
| property_fields           | array   | Conditional | -       | Property mappings and target data types. Required for user records. |
| event_name                | string  | Conditional | -       | Event name or `${field_name}` expression. Required when `record_type = "events"`. |
| time_column               | string  | Conditional | -       | Event time column. Required when `record_type = "events"`. Use `current_time()` to use processing time. |
| detail_id_column          | string  | Conditional | -       | Detail ID column. Required when `record_type = "details"`. |
| item_id_column            | string  | Conditional | -       | Item ID column. Required when `record_type = "items"`. |
| item_type_column          | string  | Conditional | -       | Item type column. Required when `record_type = "items"`. |
| time_free                 | boolean | No          | false   | Whether to enable SensorsData historical data mode. |
| skip_error_record         | boolean | No          | false   | Whether to skip records that fail conversion. |
| instant_events            | array   | No          | []      | Event names that should be marked as instant events. |
| distinct_id_by_identities | boolean | No          | false   | Fill distinct ID from `identity_fields` when `distinct_id_column` is null. |
| null_as_profile_unset     | boolean | No          | false   | Convert null profile properties to profile-unset operations. |
| common-options            | config  | No          | -       | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md). |


## Parameter Interpretation

### Record type requirements

- For user events, set `entity_name = "users"` and `record_type = "events"`, then configure
  `event_name`, `time_column`, `distinct_id_column`, `identity_fields`, and `property_fields`.
- For user details, set `entity_name = "users"` and `record_type = "details"`, then configure
  `detail_id_column`, `distinct_id_column`, `identity_fields`, and `property_fields`.
- For item records, set `entity_name = "items"` and `record_type = "items"`, then configure
  `item_id_column`, `item_type_column`, and `property_fields`.

### server_url [string]

SensorsData data sink address, the format is `https://${host}:8106/sa?project=${project}`

### bulk_size [int]

Threshold for the triggering flush operation in SensorsData SDK. When the memory cache queue reaches this value, the data in the cache will be sent. The default value is 50.

### max_cache_row_size [int]

Maximum cache refresh size for SensorsData SDK. If it exceeds this value, the flush operation will be triggered immediately. The default value is 0, which depends on bulkSize.

### consumer [string]

When consumer is set to "console", the data will be output to console instead of send to the server.

### entity_name [string]

The entity name of the SensorsData entity data model to receive the data records.

### record_type [string]

The record type of the SensorsData entity data model.

### schema [string]

The schema name of the SensorsData entity data model.

### distinct_id_column [string]

The distinct id column of the user entity.

### identity_fields [array]

The identity fields of the user entity.

### property_fields [array]

The property fields of the data record. Supported types:
- BOOLEAN
- DECIMAL
- INT
- BIGINT
- FLOAT
- DOUBLE
- NUMBER
- STRING
- DATE
- TIMESTAMP
- LIST
- LIST_COMMA
- LIST_SEMICOLON

### event_name [string]

Currently, two formats are supported:

1. Fill in the name of the event record.
2. Use value of a field from upstream data as the event name, the format is `${your field name}`, where event name is the value of the columns of the upstream data.

For example, Upstream data is the following:

|   name   | prop1 |     prop2     |
|----------|-------|---------------|
| Purchase | 16    | data-example1 |
| Order    | 23    | data-example2 |

If `${name}` is set as the event name, the event name of the first row is "Purchase", and the event name of the second row is "Order".

### time_column [string]

The time column of the event record.

### time_free [boolean]

Enable historical data mode.

### detail_id_column [string]

The detail id column of the user entity.

### item_id_column [string]

The item id column of the item entity.

### item_type_column [string]

The item type column of the item entity.

### skip_error_record [boolean]

Whether ignore the error in translating the data record.

### instant_events [array]

Given a list of event names, mark the event as an instant event.

### distinct_id_by_identities [boolean]

When enabled, this option automatically fills the distinct_id using the values from identity_fields columns when the distinct_id_column value is null. This ensures that SensorsData receives a non-null distinct_id value as required.

### null_as_profile_unset [boolean]

When enabled, null values in profile properties will be converted to profile unset operations, effectively removing the existing value from the profile.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

## Examples

### Basic Event Tracking

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

### Dynamic Event Names

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    time_free = true

    record_type = events
    schema = events
    event_name = "${event_type}"  # Use dynamic event name from data
    time_column = event_timestamp
    distinct_id_column = user_id
    identity_fields = [
      { source = user_id, target = "$identity_login_id" }
      { source = user_id, target = "$identity_distinct_id" }
    ]
    property_fields = [
      { target = "price", source = amount, type = DECIMAL }
      { target = "category", source = product_category, type = STRING }
      { target = "device", source = device_type, type = STRING }
    ]
    instant_events = ["$AppStart", "$AppEnd"]  # Mark specific events as instant
  }
}
```

### Profile Property Updates

User profile records (`record_type = "users"`) update the attributes attached to a SensorsData user
profile. Setting `null_as_profile_unset = true` makes null properties delete the corresponding
profile attribute instead of leaving the previous value in place.

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    time_free = true

    entity_name = users
    record_type = users
    schema = users
    distinct_id_column = user_id
    identity_fields = [
      { source = email, target = "$identity_email" }
      { source = phone, target = "$identity_phone" }
    ]
    property_fields = [
      { target = "name", source = full_name, type = STRING }
      { target = "age", source = user_age, type = INT }
      { target = "gender", source = user_gender, type = STRING }
      { target = "location", source = user_location, type = STRING }
    ]
    null_as_profile_unset = true  # Remove properties when null
  }
}
```

### Item Tracking

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    time_free = true

    record_type = items
    schema = items
    event_name = "$ItemViewed"
    time_column = view_time
    distinct_id_column = user_id
    identity_fields = [
      { source = user_id, target = "$identity_login_id" }
    ]
    property_fields = [
      { target = "view_duration", source = duration, type = INT }
      { target = "referrer", source = referrer_url, type = STRING }
    ]
    item_id_column = product_id
    item_type_column = product_type
  }
}
```

### User Details (Detail Records)

User detail records (`record_type = "details"`) attach a per-record identifier to a SensorsData
user. The `detail_id_column` provides that detail key, while `distinct_id_column` and
`identity_fields` identify the parent user.

```hocon
sink {
  SensorsData {
    consumer = console
    server_url = "http://10.129.27.43:8106/sa?project=sditest"
    time_free = true

    record_type = details
    schema = fund_manager
    distinct_id_column = c_id
    detail_id_column = c_id
    identity_fields = [
      { target = "$identity_distinct_id", source = c_id }
    ]
    property_fields = [
      { target = c_id, source = c_id, type = STRING }
      { target = fund_amount, source = c_int, type = INT }
      { target = "$is_valid", source = c_boolean, type = BOOLEAN }
    ]
  }
}
```

### Dynamic Event Names With Multiple Identities

This example builds the event name from the data row itself (`event_name = "${c_event}"`) and
maps several identities at once (`$identity_login_id`, `$identity_distinct_id`). Records that fail
to convert are skipped via `skip_error_record = true`.

```hocon
sink {
  SensorsData {
    consumer = console
    server_url = "http://10.1.136.63:8106/sa?project=default"
    time_free = true

    record_type = events
    schema = events
    event_name = "${c_event}"
    time_column = c_date
    distinct_id_column = c_bigint
    identity_fields = [
      { source = c_bigint, target = "$identity_login_id" }
      { source = c_bigint, target = "$identity_distinct_id" }
    ]
    property_fields = [
      { target = c_tinyint, source = c_tinyint, type = INT }
      { target = c_bigint,   source = c_bigint,   type = BIGINT }
      { target = c_int,      source = c_int,      type = INT }
      { target = c_boolean,  source = c_boolean,  type = BOOLEAN }
    ]
    skip_error_record = true
  }
}
```

### Console Output (for Testing)

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    consumer = "console"  # Output to console instead of sending to server
    record_type = events
    schema = events
    event_name = "$TestEvent"
    time_column = timestamp
    distinct_id_column = test_id
    property_fields = [
      { target = "test", source = test_field, type = STRING }
    ]
  }
}
```

## Changelog

<ChangeLog />

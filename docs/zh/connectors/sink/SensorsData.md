import ChangeLog from '../changelog/connector-sensorsdata.md';

# SensorsData

> SensorsData Sink 连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

## 描述

一个 Sink 插件，使用 SensorsData SDK 发送数据记录。

## Sink 选项

| 参数名                      | 类型    | 必须 | 默认值 |
|---------------------------|---------|------|--------|
| server_url                | string  | 是   | -      |
| bulk_size                 | int     | 否   | 50     |
| max_cache_row_size        | int     | 否   | 0      |
| consumer                  | string  | 否   | batch  |
| entity_name               | string  | 是   | users  |
| record_type               | string  | 是   | users  |
| schema                    | string  | 是   | users  |
| distinct_id_column        | string  | 是   | -      |
| identity_fields           | array   | 是   | -      |
| property_fields           | array   | 是   | -      |
| event_name                | string  | 是   | -      |
| time_column               | string  | 是   | -      |
| time_free                 | boolean | 否   | false  |
| detail_id_column          | string  | 否   | -      |
| item_id_column            | string  | 否   | -      |
| item_type_column          | string  | 否   | -      |
| skip_error_record         | boolean | 否   | false  |
| instant_events            | array   | 否   | -      |
| distinct_id_by_identities | boolean | 否   | false  |
| null_as_profile_unset     | boolean | 否   | false  |
| common-options            |         | 否   | -      |


## 参数解释
### server_url [string]

SensorsData 数据 Sink 地址，格式为 `https://${host}:8106/sa?project=${project}`

### bulk_size [int]

SensorsData SDK 中触发刷新操作的阈值。当内存缓存队列达到此值时，缓存中的数据将被发送。默认值为 50。

### max_cache_row_size [int]

SensorsData SDK 的最大缓存刷新大小。如果超过此值，将立即触发刷新操作。默认值为 0，取决于 bulkSize。

### consumer [string]

当 consumer 设置为 "console" 时，数据将输出到控制台而不是发送到服务器。

### entity_name [string]

接收数据记录的 SensorsData 实体数据模型的实体名称。

### record_type [string]

SensorsData 实体数据模型的记录类型。

### schema [string]

SensorsData 实体数据模型的模式名称。

### distinct_id_column [string]

用户实体的 distinct id 列。

### identity_fields [array]

用户实体的身份字段。

### property_fields [array]

数据记录的属性字段。支持的类型：
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

目前支持两种格式：

1. 填入事件记录的名称。
2. 使用来自上游数据的字段值作为事件名称，格式为 `${your field name}`，其中事件名称是上游数据列的值。

例如，上游数据如下：

|   name   | prop1 |     prop2     |
|----------|-------|---------------|
| Purchase | 16    | data-example1 |
| Order    | 23    | data-example2 |

如果将 `${name}` 设置为事件名称，第一行的事件名称为 "Purchase"，第二行的事件名称为 "Order"。

### time_column [string]

事件记录的时间列。

### time_free [boolean]

启用历史数据模式。

### detail_id_column [string]

用户实体的详细 id 列。

### item_id_column [string]

项目实体的项目 id 列。

### item_type_column [string]

项目实体的项目类型列。

### skip_error_record [boolean]

是否忽略转换数据记录中的错误。

### instant_events [array]

给定事件名称列表，将事件标记为即时事件。

### distinct_id_by_identities [boolean]

启用后，此选项在 distinct_id_column 值为 null 时，自动使用 identity_fields 列中的值填充 distinct_id。这确保 SensorsData 接收到所需的非 null distinct_id 值。

### null_as_profile_unset [boolean]

启用后，配置文件属性中的 null 值将转换为配置文件取消设置操作，有效地从配置文件中删除现有值。

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](common-options.md) 详见

## 示例

### 基本事件跟踪

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

### 动态事件名称

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    time_free = true

    record_type = events
    schema = events
    event_name = "${event_type}"  # 使用来自数据的动态事件名称
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
    instant_events = ["$AppStart", "$AppEnd"]  # 将特定事件标记为即时事件
  }
}
```

### 配置文件属性更新

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    time_free = true

    entity_name = users
    record_type = profile
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
    null_as_profile_unset = true  # 当为 null 时删除属性
  }
}
```

### 项目跟踪

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

### 控制台输出（用于测试）

```hocon
sink {
  SensorsData {
    server_url = "http://10.1.136.63:8106/sa?project=default"
    consumer = "console"  # 输出到控制台而不是发送到服务器
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

## 变更日志

<ChangeLog />



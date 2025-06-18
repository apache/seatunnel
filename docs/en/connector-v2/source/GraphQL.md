import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL source connector

## Description

Used to read data from GraphQL.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)

## Options

| name                        | type    | required | default value           |
| --------------------------- | ------- | -------- | ----------------------- |
| url                         | String  | Yes      | -                       |
| query                       | String  | Yes      | -                       |
| variables                   | Config  | No       | -                       |
| enable_subscription         | boolean | No       | false                   |
| timeout                     | Long    | No       | -                       |
| content_field               | String  | Yes      | $.data.{query_object}.* |
| schema.fields               | Config  | Yes      | -                       |
| format                      | String  | No       | json                    |
| params                      | Map     | Yes      | -                       |
| poll_interval_millis        | int     | No       | -                       |
| retry                       | int     | No       | -                       |
| retry_backoff_multiplier_ms | int     | No       | 100                     |
| retry_backoff_max_ms        | int     | No       | 10000                   |
| enable_multi_lines          | boolean | No       | false                   |
| common-options              | config  | No       | -                       |

### url [String]

http request url

### query [String]

GraphQL expression query string

### variables [String]

GraphQL Variables

for example 

```
variables = {
   limit = 2
}
```

### enable_subscription [boolean]

1. true :  Build a socket reader to subscribe to the GraphQL service
2. false :  Build an http reader subscription to the GraphQL service

### timeout [Long]

Time-out Period

### content_field [String]

JSONPath wildcard

### params [Map]

http request params

### poll_interval_millis [int]

request http api interval(millis) in stream mode

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

The retry-backoff times(millis) multiplier if request http failed

### retry_backoff_max_ms [int]

The maximum retry-backoff times(millis) if request http failed

### format [String]

the format of upstream data, default `json`.

### schema [Config]

Fill in a fixed value

```hocon
    schema = {
        fields {
            metric = "map<string, string>"
            value = double
            time = long
            }
        }

```

#### fields [Config]

the schema fields of upstream data

### common options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details

## Example

### Query

```hocon
source {
    GraphQL {
        url = "http://192.168.1.103:9081/v1/graphql"
        format = "json"
        content_field = "$.data.source"
        query = """
            query MyQuery($limit: Int) {
                source(limit: $limit) {
                    id
                    val_bool
                    val_double
                    val_float
                }
            }
        """
        variables = {
            limit = 2
        }
        schema = {
            fields {
               id = "int"
               val_bool = "boolean"
               val_double = "double"
               val_float = "float"
            }
        }
    }
}
```

### Subscription

```hocon
source {
    GraphQL {
        url = "http://192.168.1.103:9081/v1/graphql"
        format = "json"
        content_field = "$.data.source"
        query = """
            query MyQuery($limit: Int) {
                source(limit: $limit) {
                    id
                    val_bool
                    val_double
                    val_float
                }
            }
        """
        variables = {
            limit = 2
        }
        enable_subscription = true
        schema = {
            fields {
               id = "int"
               val_bool = "boolean"
               val_double = "double"
               val_float = "float"
            }
        }
    }
}
```

## Changelog

<ChangeLog />

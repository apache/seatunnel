import ChangeLog from '../changelog/connector-http-myhours.md';

# My Hours

> My Hours source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The My Hours source exposes settings for reading My Hours REST endpoints through the shared HTTP request and retry runtime.

:::caution Authentication limitation

Automatic email/password authentication currently sends the configured data request instead of the login request containing credentials. The template below using a data URL is a configuration reference and does not complete that login. A separately tested connector authentication fix is required before live use; access tokens are not automatically refreshed.

:::

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip

The shared HTTP reader supports repeated polling in streaming mode using `poll_interval_millis`. The authentication limitation above applies to My Hours in both batch and streaming modes.

:::

## Supported DataSource Info

In order to use the My Hours connector, the following dependency is required.
It can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                         Dependency                                          |
|------------|--------------------|---------------------------------------------------------------------------------------------|
| My Hours   | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-myhours)      |

## Source Options

|            Name             |  Type   | Required | Default | Description                                                                                                           |
|-----------------------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------------------------|
| url                         | String  | Yes      | -       | My Hours API request URL.                                                                                             |
| email                       | String  | Yes      | -       | My Hours login email address.                                                                                         |
| password                    | String  | Yes      | -       | My Hours login password.                                                                                              |
| schema                      | Config  | No       | -       | Required when `format` is `json`. For more details, see [Schema Feature](../../introduction/concepts/schema-feature.md). |
| schema.fields               | Config  | No       | -       | The schema fields of upstream data.                                                                                   |
| json_field                  | Config  | No       | -       | Extract fields from a JSON response by JSONPath. Use it together with `schema`.                                       |
| content_field               | String  | No       | -       | Extract one part of a JSON response, such as `$.store.book.*`, before schema parsing.                                 |
| format                      | String  | No       | text    | Response format. Supports `json` and `text`. Set `json` when using `schema`, `json_field`, or `content_field`.        |
| method                      | String  | No       | GET     | HTTP request method. Supports `GET` and `POST`.                                                                       |
| headers                     | Map     | No       | -       | Extra HTTP headers. The connector adds the My Hours `Authorization` header after login.                               |
| params                      | Map     | No       | -       | HTTP query parameters.                                                                                                |
| body                        | String  | No       | -       | HTTP request body.                                                                                                    |
| poll_interval_millis        | Int     | No       | -       | Request interval in milliseconds for stream mode.                                                                      |
| retry                       | Int     | No       | -       | Maximum retry times when the request throws `IOException`.                                                            |
| retry_backoff_multiplier_ms | Int     | No       | 100     | Retry backoff multiplier in milliseconds.                                                                             |
| retry_backoff_max_ms        | Int     | No       | 10000   | Maximum retry backoff in milliseconds.                                                                                |
| json_filed_missed_return_null | Boolean | No     | false   | Return `null` when a configured JSON field is missing.                                                                |
| common-options              |         | No       | -       | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md).             |

## Configuration reference (authentication currently blocked)

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  MyHours {
    url = "https://api2.myhours.com/api/Projects/getAll"
    email = "seatunnel@test.com"
    password = "********"
    method = "GET"
    format = "json"
    schema {
       fields {
         name = string
         archived = boolean
         dateArchived = string
         dateCreated = string
         clientName = string
         budgetAlertPercent = string
         budgetType = int
         totalTimeLogged = double
         budgetValue = double
         totalAmount = double
         totalExpense = double
         laborCost = double
         totalCost = double
         billableTimeLogged = double
         totalBillableAmount = double
         billable = boolean
         roundType = int
         roundInterval = int
         budgetSpentPercentage = double
         budgetTarget = int
         budgetPeriodType = string
         budgetSpent = string
         id = string
       }
    }
  }
}

# Console printing of the read data
sink {
  Console {
    parallelism = 1
  }
}
```

## Parameter Interpretation

### Authentication

The configuration requires `email` and `password`, but the current implementation has the authentication limitation described above. No automatic access-token refresh is implemented.

### Request scope and polling limits

Each configured request reads one HTTP response. Automatic pagination is not supported: `pageing` does not advance pages or follow next-page links or cursors. Select a specific page in the API request or manage multiple requests outside the connector.

Streaming mode repeats the configured request. Polls can re-emit the same rows, and the connector does not checkpoint an API cursor for incremental recovery. Repeated polls and restarts can therefore produce duplicates.

### format

when you assign format is `json`, you should also assign schema option, for example:

upstream data is the following:

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

you should assign schema as the following:

```hocon

schema {
  fields {
    code = int
    data = string
    success = boolean
  }
}

```

connector will generate data as the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

when you assign format is `text`, connector will do nothing for upstream data, for example:

upstream data is the following:

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

connector will generate data as the following:

|                         content                          |
|----------------------------------------------------------|
| {"code":  200, "data":  "get success", "success":  true} |

### content_field

This parameter can get some json data.If you only need the data in the 'book' section, configure `content_field = "$.store.book.*"`.

If your return data looks something like this.

```json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

You can configure `content_field = "$.store.book.*"` and the result returned looks like this:

```json
[
  {
    "category": "reference",
    "author": "Nigel Rees",
    "title": "Sayings of the Century",
    "price": 8.95
  },
  {
    "category": "fiction",
    "author": "Evelyn Waugh",
    "title": "Sword of Honour",
    "price": 12.99
  }
]
```

Then you can get the desired result with a simpler schema,like

```hocon
MyHours {
  url = "http://mockserver:1080/contentjson/mock"
  email = "seatunnel@test.com"
  password = "********"
  method = "GET"
  format = "json"
  content_field = "$.store.book.*"
  schema = {
    fields {
      category = string
      author = string
      title = string
      price = string
    }
  }
}
```

### json_field

This parameter helps you configure the schema,so this parameter must be used with schema.

If your data looks something like this:

```json
{ 
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

You can get the contents of 'book' by configuring the task as follows:

```hocon
source {
  MyHours {
    url = "http://mockserver:1080/jsonpath/mock"
    email = "seatunnel@test.com"
    password = "********"
    method = "GET"
    format = "json"
    json_field = {
      category = "$.store.book[*].category"
      author = "$.store.book[*].author"
      title = "$.store.book[*].title"
      price = "$.store.book[*].price"
    }
    schema = {
      fields {
        category = string
        author = string
        title = string
        price = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />

# OneSignal

> OneSignal source connector

## Description

Used to read data from OneSignal.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

##  Options

| name                        | type   | required | default value |
| --------------------------- | ------ | -------- | ------------- |
| url                         | String | Yes      | -             |
| password                    | String | Yes      | -             |
| method                      | String | No       | get           |
| schema                      | Config | No       | -             |
| schema.fields               | Config | No       | -             |
| format                      | String | No       | json          |
| params                      | Map    | No       | -             |
| body                        | String | No       | -             |
| poll_interval_ms            | int    | No       | -             |
| retry                       | int    | No       | -             |
| retry_backoff_multiplier_ms | int    | No       | 100           |
| retry_backoff_max_ms        | int    | No       | 10000         |
| common-options              | config | No       | -             |

### url [String]

http request url

### password [String]

Auth key for login, you can get more detail at this link:

https://documentation.onesignal.com/docs/accounts-and-keys#user-auth-key

### method [String]

http request method, only supports GET, POST method

### params [Map]

http params

### body [String]

http body

### poll_interval_ms [int]

request http api interval(millis) in stream mode

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

The retry-backoff times(millis) multiplier if request http failed

### retry_backoff_max_ms [int]

The maximum retry-backoff times(millis) if request http failed

### format [String]

the format of upstream data, now only support `json` `text`, default `json`.

when you assign format is `json`, you should also assign schema option, for example:

upstream data is the following:

```json

{"code":  200, "data":  "get success", "success":  true}

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

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

when you assign format is `text`, connector will do nothing for upstream data, for example:

upstream data is the following:

```json

{"code":  200, "data":  "get success", "success":  true}

```

connector will generate data as the following:

| content |
|---------|
| {"code":  200, "data":  "get success", "success":  true}        |

### schema [Config]

#### fields [Config]

the schema fields of upstream data

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example

```hocon

OneSignal {
    url = "https://onesignal.com/api/v1/apps"
    password = "Seatunnel-test"
    schema = {
       fields {
         id = string
         name = string
         gcm_key = string
         chrome_key = string
         chrome_web_key = string
         chrome_web_origin = string
         chrome_web_gcm_sender_id = string
         chrome_web_default_notification_icon = string
         chrome_web_sub_domain = string
         apns_env = string
         apns_certificates = string
         apns_p8 = string
         apns_team_id = string
         apns_key_id = string
         apns_bundle_id = string
         safari_apns_certificate = string
         safari_site_origin = string
         safari_push_id = string
         safari_icon_16_16 = string
         safari_icon_32_32 = string
         safari_icon_64_64 = string
         safari_icon_128_128 = string
         safari_icon_256_256 = string
         site_name = string
         created_at = string
         updated_at = string
         players = int
         messageable_players = int
         basic_auth_key = string
         additional_data_is_root_payload = string
       }
    }   
}
```

## Changelog

### next version

- Add OneSignal Source Connector

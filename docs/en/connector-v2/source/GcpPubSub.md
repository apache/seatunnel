# GcpPubsub

> GcpPubsub source connector

## Description

Used to read data from Google Cloud Platform Pubsub.

## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [ ] file format
    - [ ] text
    - [ ] csv
    - [ ] json

## Options

| name                | type   | required | default value |
|---------------------|--------|----------|---------------|
| service_account_key | string | yes      | -             |
| project_id          | string | yes      | -             |
| subscription_id     | string | yes      | -             |
| topic               | string | yes      | -             |
| schema              | config | no       | -             |

### service_account_key [string]

google cloud service account, base64 required

### project_id [string]

Project id of Google Pubsub Service.

### subscription_id [string]

Subscription id of Google Pubsub Service.

### topic [string]

Topic of Google Pubsub Service.

### schema [config]

#### fields [config]

the schema fields of upstream data

## Example

simple:

```hocon
  GcpPubSub {
    service_account_keys = "${service_account_id}"
    project_id = "${project_id}"
    subscription_id = "${subscription_id}"
    topic = "${topic}"
    schema = {
      fields {
        a = string
        b = int
        c = string
      }
    }
  }
```

## Changelog

### next version

- Add GcpPubsub Source Connector


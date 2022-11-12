# Redis

> GoogleSheets sink connector

## Description

Used to write data to GoogleSheets.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

## Options

| name                | type         | required | default value |
|-------------------  |--------------|----------|---------------|
| service_account_key | string       | yes      | -             |
| sheet_id            | string       | yes      | -             |
| sheet_name          | string       | yes      | -             |
| range               | string       | yes      | -             |

### service_account_key [string]

google cloud service account, base64 required

### sheet_id [string]

sheet id in a Google Sheets URL

### sheet_name [string]

the name of the sheet you want to output

### range [string]

the range of the sheet you want to output

## Example

simple:

```hocon
  GoogleSheets {
    service_account_key = "seatunnel-test"
    sheet_id = "1VI0DvyZK-NIdssSdsDSsSSSC-_-rYMi7ppJiI_jhE"
    sheet_name = "sheets01"
    range = "A1:C3"
  }
```

## Changelog

### next version

- Add GoogleSheets Sink Connector

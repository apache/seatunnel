# GoogleSheets

> GoogleSheets source connector

## Description

Used to read data from GoogleSheets.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [ ] file format
    - [ ] text
    - [ ] csv
    - [ ] json

## Options

| name                | type         | required | default value |
|-------------------  |--------------|----------|---------------|
| service_account_key | string       | yes      | -             |
| sheet_id            | string       | yes      | -             |
| sheet_name          | string       | yes      | -             |
| range               | string       | yes      | -             |
| schema              | config       | no       | -             |

### service_account_key [string]

google cloud service account, base64 required

### sheet_id [string]

sheet id in a Google Sheets URL

### sheet_name [string]

the name of the sheet you want to import

### range [string]

the range of the sheet you want to import

### schema [config]

#### fields [config]

the schema fields of upstream data

## Example

simple:

```hocon
  GoogleSheets {
    service_account_key = "seatunnel-test"
    sheet_id = "1VI0DvyZK-NIdssSdsDSsSSSC-_-rYMi7ppJiI_jhE"
    sheet_name = "sheets01"
    range = "A1:C3"
    schema = {
      fields {
        a = int
        b = string
        c = string
      }
    }
  }
```


## Changelog

### next version

- Add GoogleSheets Source Connector

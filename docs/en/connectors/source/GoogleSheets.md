import ChangeLog from '../changelog/connector-google-sheets.md';

# GoogleSheets

> GoogleSheets source connector

## Description

Used to read data from Google Sheets through the Google Sheets API. The connector reads a configured
range from a sheet using a Google Cloud service account and converts each row into a SeaTunnel record
based on the user-defined schema.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [ ] file format
  - [ ] text
  - [ ] csv
  - [ ] json

## Data Type Mapping

The Google Sheets API does not expose per-cell types — every cell comes back as an untyped raw value.
The connector casts each cell according to the user-declared `schema` option, so the resulting
SeaTunnel type is driven entirely by your schema, not by any type detected from the sheet itself.
Cells with values that cannot be cast to the configured schema field will cause the connector to fail
the row.

| Google Sheets Cell | SeaTunnel Data Type (after schema cast) |
|--------------------|----------------------------------------|
| string             | string / numeric / boolean / date      |
| number             | int / long / float / double            |
| boolean            | boolean                                |
| date               | date / time / timestamp                |

## Source Options

|        name         |  type  | required | default value | description                                                                                       |
|---------------------|--------|----------|---------------|---------------------------------------------------------------------------------------------------|
| service_account_key | string | yes      | -             | Google Cloud service account credentials. Must be provided as a Base64-encoded JSON string.       |
| sheet_id            | string | yes      | -             | The sheet id of the Google Sheets URL, for example `1VI0DvyZK-NIdssSdsDSsSSSC-_-rYMi7ppJiI_jhE`.   |
| sheet_name          | string | yes      | -             | The name of the sheet (tab) inside the Google Sheets document to read from.                       |
| range               | string | yes      | -             | The A1 notation range to read from the sheet, for example `A1:C3` or `Sheet1!A1:D100`.           |
| schema              | config | no       | -             | The schema of the rows emitted by the connector. See [Schema Feature](../../introduction/concepts/schema-feature.md). |

### service_account_key [string]

The Base64-encoded JSON content of a Google Cloud service account key file. The service account must
have access to the target Google Sheets document (share the sheet with the service account email).

### sheet_id [string]

The id of the Google Sheets document. It is the long identifier between `/d/` and `/edit` in the
sheet's URL.

### sheet_name [string]

The name of the sheet (tab) inside the Google Sheets document to read from, for example `Sheet1`.

### range [string]

The A1 notation range to read from the sheet, for example `A1:C3` to read a fixed area or `Sheet1!A:D`
to read entire columns from a specific sheet.

### schema [config]

#### fields [config]

The schema fields of upstream data. The connector reads each cell as a string and casts it to the
declared field type. Please refer to [Schema Feature](../../introduction/concepts/schema-feature.md) for
the available types.

## Task Example

### Simple

```hocon
source {
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
}
```

### With downstream sink

Read a sheet and print the rows through the Console sink.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleSheets {
    service_account_key = "seatunnel-test"
    sheet_id = "1VI0DvyZK-NIdssSdsDSsSSSC-_-rYMi7ppJiI_jhE"
    sheet_name = "sheets01"
    range = "A1:C100"
    schema = {
      fields {
        a = int
        b = string
        c = string
      }
    }
  }
}

sink {
  Console {
  }
}
```

## Changelog

<ChangeLog />

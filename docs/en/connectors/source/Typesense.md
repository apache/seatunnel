import ChangeLog from '../changelog/connector-typesense.md';

# Typesense

> Typesense Source Connector

## Description

Reads documents from a Typesense collection. The source supports bounded batch reads and can pass
Typesense search parameters through `query`.

## Key Features

- [x] [Batch Processing](../../introduction/concepts/connector-v2-features.md)
- [ ] [Stream Processing](../../introduction/concepts/connector-v2-features.md)
- [ ] [Exactly-Once](../../introduction/concepts/connector-v2-features.md)
- [x] [Schema](../../introduction/concepts/connector-v2-features.md)
- [x] [Parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [User-Defined Splits Support](../../introduction/concepts/connector-v2-features.md)

## Options

|    Name    |  Type  | Required | Default |
|------------|--------|----------|---------|
| hosts      | array  | yes      | -       |
| collection | string | yes      | -       |
| schema     | config | yes      | -       |
| api_key    | string | yes      | -       |
| protocol   | string | no       | http    |
| query      | string | no       | -       |
| batch_size | int    | no       | 100     |
| common-options |      | no       | -       |

### hosts [array]

The access address of Typesense. Use the `host:port` format, for example:
`["typesense-01:8108"]`. Multiple hosts are supported.

### collection [string]

The name of the Typesense collection to read from, for example: `"companies"`.

### schema [config]

The columns to be read from Typesense. For more information, please refer to the [guide](../../introduction/concepts/schema-feature.md#how-to-declare-type-supported).

### api_key [string]

The `api_key` for Typesense security authentication.

### protocol [string]

The protocol used to connect to Typesense. The default value is `http`. Use `https` for
Typesense Cloud or other TLS-enabled endpoints.

### query [string]

Typesense search parameters, for example `q=*&filter_by=num_employees:>9000`. If it is not set,
the source reads all documents returned by the default search.

### batch_size [int]

The number of records to query per batch when reading data.

### Common Options

For common parameters of Source plugins, please refer to [Source Common Options](../common-options/source-common-options.md).

## Task Example

### Read Documents With A Filter

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Typesense {
    hosts = ["localhost:8108"]
    collection = "companies"
    api_key = "xyz"
    query = "q=*&filter_by=num_employees:>9000"
    batch_size = 100
    schema = {
      fields {
        company_name_list = array<string>
        company_name = string
        num_employees = long
        country = string
        id = string
        c_row = {
          c_int = int
          c_string = string
          c_array_int = array<int>
        }
      }
    }
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />

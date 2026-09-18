import ChangeLog from '../changelog/connector-typesense.md';

# Typesense

> Typesense Source Connector

## Support Those Engines

> SeaTunnel Zeta<br/>

## Description

Reads documents from a Typesense collection. The source supports bounded batch reads and can pass
Typesense search parameters through `query`.

The source is bounded. Each job reads every document that matches the configured `query` once and then completes. Use Typesense's own change tracking on the receiver side if you need change-data capture.

## Key Features

- [x] [Batch Processing](../../introduction/concepts/connector-v2-features.md)
- [ ] [Stream Processing](../../introduction/concepts/connector-v2-features.md)
- [ ] [Exactly-Once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
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
`["typesense-01:8108"]`. Multiple hosts are supported. When several nodes are configured, the source issues its search requests to the first reachable node; the list is not used for parallel scan sharding.

### collection [string]

The name of the Typesense collection to read from, for example: `"companies"`.

### schema [config]

The columns to be read from Typesense. For more information, please refer to the [guide](../../introduction/concepts/schema-feature.md#how-to-declare-type-supported).

### api_key [string]

The `api_key` for Typesense security authentication. Treat this value as a secret and prefer passing it via a job secret or environment variable when running on shared infrastructure.

### protocol [string]

The protocol used to connect to Typesense. The default value is `http`. Use `https` for
Typesense Cloud or other TLS-enabled endpoints.

### query [string]

Typesense search parameters, for example `q=*&filter_by=num_employees:>9000`. If it is not set,
the source reads all documents returned by the default search.

Any valid Typesense search parameter can be appended, including `q`, `query_by`, `filter_by`, `sort_by`, `page`, and `per_page`. The connector forwards them to the Typesense search API unchanged.

### batch_size [int]

The number of records to query per batch when reading data. Each request uses the Typesense `per_page` parameter, so the value must be between 1 and the Typesense server-side `per_page` limit (typically 250). Lower the value if you see truncated pages in the logs.

### Common Options

For common parameters of Source plugins, please refer to [Source Common Options](../common-options/source-common-options.md).

## Task Example

### Read Documents With A Filter

```hocon
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

### Read A Subset With A Custom Query

Combine `query_by` and `sort_by` to control which fields Typesense searches and how the result set is ordered.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Typesense {
    hosts = ["localhost:8108"]
    collection = "companies"
    api_key = "xyz"
    query = "q=acme&query_by=company_name&filter_by=country:=US&sort_by=num_employees:desc"
    batch_size = 50
    schema = {
      fields {
        company_name = string
        num_employees = long
        country = string
        id = string
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

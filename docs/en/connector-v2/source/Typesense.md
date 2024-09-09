# Typesense

> Typesense Source Connector

## Description

Reads data from Typesense.

## Key Features

- [x] [Batch Processing](../../concept/connector-v2-features.md)
- [ ] [Stream Processing](../../concept/connector-v2-features.md)
- [ ] [Exactly-Once](../../concept/connector-v2-features.md)
- [x] [Schema](../../concept/connector-v2-features.md)
- [x] [Parallelism](../../concept/connector-v2-features.md)
- [ ] [User-Defined Splits Support](../../concept/connector-v2-features.md)

## Options

|    Name    |  Type  | Required | Default |
|------------|--------|----------|---------|
| hosts      | array  | yes      | -       |
| collection | string | yes      | -       |
| schema     | config | yes      | -       |
| api_key    | string | no       | -       |
| query      | string | no       | -       |
| batch_size | int    | no       | 100     |

### hosts [array]

The access address of Typesense, for example: `["typesense-01:8108"]`.

### collection [string]

The name of the collection to write to, for example: `"seatunnel"`.

### schema [config]

The columns to be read from Typesense. For more information, please refer to the [guide](../../concept/schema-feature.md#how-to-declare-type-supported).

### api_key [config]

The `api_key` for Typesense security authentication.

### batch_size

The number of records to query per batch when reading data.

### Common Options

For common parameters of Source plugins, please refer to [Source Common Options](../source-common-options.md).

## Example

```bash
source {
   Typesense {
      hosts = ["localhost:8108"]
      collection = "companies"
      api_key = "xyz"
      query = "q=*&filter_by=num_employees:>9000"
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
```


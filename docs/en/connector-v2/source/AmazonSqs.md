# AmazonSqs

> AmazonSqs source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Read data from Amazon SQS.

## Source Options

|      name      |  type  | required | default value |
|----------------|--------|----------|---------------|
| url            | string | yes      | -             |
| region         | string | false    | -             |
| queue          | string | false    | -             |
| schema         | config | yes      | -             |
| common-options |        | yes      | -             |

### url [string]

The URL to read from Amazon SQS.

### queue [string]

the queue name to read from Amazon SQS.

### region [string]

The region of Amazon SQS.

### schema [Config]

#### fields [config]

Amazon SQS is a managed message queuing service provided by AWS. the schema and fields are required to deserialize the data from
queue into SeaTunnel rows.
such as:

```
schema {
  fields {
    id = int
    key_aa = string
    key_bb = string
  }
}
```

### common options

Source Plugin common parameters, refer to [Source Plugin](common-options.md) for details

## Task Example

```bash
source {
  amazonsqs {
    url = "http://127.0.0.1:4566"
    region = "us-east-1"
    queue = "QueueName"
    schema = {
      fields {
        artist = string
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

## Changelog

### next version


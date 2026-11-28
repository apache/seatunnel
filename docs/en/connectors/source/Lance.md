import ChangeLog from '../changelog/connector-lance.md';

# Lance

> Lance source connector

## Support These Engines

> Spark 3.4 and later<br/>
> Flink is not supported by this connector<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Description

The Lance source reads rows from an existing Lance dataset. It reads the latest version of the
dataset in batch mode, one split per dataset fragment, and derives the SeaTunnel schema from the
dataset's Arrow schema.

The connector currently works with directory-based Lance namespaces, including local filesystem
paths and object storage paths such as `s3://` (with `lance.read.storage.options` for
credentials).

## Using Dependency

```xml
<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lance-core</artifactId>
    <version>0.33.0</version>
</dependency>

<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lance-namespace-core</artifactId>
    <version>0.0.14</version>
</dependency>
```

## Source Options

| Name                        | Type   | Required | Default     | Description                                                                                  |
|-----------------------------|--------|----------|-------------|----------------------------------------------------------------------------------------------|
| dataset_path                | string | no       | /test.lance | Lance dataset path. For a directory namespace, this is the path used by the Lance dataset.    |
| namespace_type              | string | no       | dir         | Lance namespace type. Currently only `dir` is supported.                                     |
| namespace_id                | string | no       | ""          | Lance namespace ID.                                                                          |
| namespace_ids               | list   | no       | []          | Lance namespace path segments.                                                              |
| root_namespace_path         | string | no       | /tmp        | Root path used by the Lance namespace.                                                       |
| table                       | string | no       | test        | Lance table name. If set, it overrides the table name derived from the dataset path.         |
| lance.read.storage.options  | map    | no       | {}          | Storage options used when opening the dataset, e.g. `aws_endpoint_url`, `aws_access_key_id`, `aws_secret_access_key`, `allow_http`. |

### dataset_path

The path of the Lance dataset to read, either a local directory path or an object storage path
such as `s3://bucket/path/to/dataset`.

If specified, this value must not be empty or contain only whitespace.

### namespace_type

The Lance namespace type. Currently the connector supports `dir`.

If specified, this value must not be empty or contain only whitespace.

### lance.read.storage.options

Storage options passed to the Lance reader when opening the dataset, following the same keys as
the sink's `lance.write.storage.options`. For MinIO or other S3-compatible object storage:

```hocon
"lance.read.storage.options" = {
  aws_endpoint_url = "http://minio:9000"
  aws_access_key_id = "admin"
  aws_secret_access_key = "password"
  allow_http = "true"
  aws_default_region = "us-east-1"
}
```

## Data Type Mapping

| Lance (Arrow) Type    | SeaTunnel Type |
|-----------------------|----------------|
| int (32 bit)          | int            |
| int (64 bit)          | bigint         |
| float                 | float          |
| double                | double         |
| utf8 / largeutf8      | string         |
| bool                  | boolean        |
| date                  | date           |
| time                 | time           |
| timestamp             | timestamp      |
| decimal               | decimal         |
| binary / largebinary  | bytes          |
| null                  | null           |
| list                  | array          |

Struct, map and other complex Arrow types are not supported and are rejected with an error that names the offending column. Fixed-size binary columns (the Arrow type Lance uses for vectors) are rejected the same way: they have no faithful SeaTunnel counterpart, so the source fails fast instead of coercing them to bytes. Lance blob columns surface as plain binary and are read as `bytes` (see the mapping table above); any other blob representation falls into the same fail-fast rejection.

The first version has the following limitations: the dataset is always opened at its latest version (no version or snapshot selection), every split scans all columns of its fragment (no column projection), and filter push-down is not supported. Column projection, version selection and filter push-down are natural follow-ups.

## Examples

### Read Lance Data To Console

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Lance {
    dataset_path = "/tmp/seatunnel_practice/users"
    namespace_type = "dir"
    table = "users"
  }
}

sink {
  Console {}
}
```

### Read Lance Data From MinIO (S3)

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Lance {
    dataset_path = "s3://lance/seatunnel_demo/users"
    namespace_type = "dir"
    table = "users"
    "lance.read.storage.options" = {
      aws_endpoint_url = "http://127.0.0.1:50073"
      aws_access_key_id = "lanceadmin"
      aws_secret_access_key = "lanceX9k2mQ7w4Tz8b"
      allow_http = "true"
      aws_default_region = "us-east-1"
    }
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />

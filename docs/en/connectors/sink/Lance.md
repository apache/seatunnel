import ChangeLog from '../changelog/connector-lance.md';

# Lance

> Lance sink connector

## Support Those Engines

> Spark(not support version under spark 3.4, reference https://lance.org/integrations/spark/install/#scala)<br/>
> Flink(not support, reference https://github.com/lance-format/lance-flink)<br/>
> SeaTunnel Zeta<br/>

## Description

Sink connector for Lance format. It can support create and write dataset 、lance namespace manage schema and version.

## Key features

- [] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Using Dependency
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

## Sink Options

| Name            | Type   | Required | Default | Description                                                                                                       |
|-----------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------------|
| dataset_path    | string | yes      | /tmp    | The dataset path for the Lance sink connection.                                                                   |
| namespace_type  | string | yes      | dir     | The namespace type of Lance dataset, now only support DirectoryNamespace, the type will be set default with "dir" |
| table           | string | yes      | test    | The name of Lance dataset, If not set, the dataset name will be set default with test                             |
| namespace_id    | string | no       | -       | The id of the lance namespace. Please refer to https://lance.org/format/namespace/                                |


## Data Type Mapping
The data type of lance depends on the Arrow data type system 

| SeaTunnel Data type | Lance Data type |
|---------------------|-----------------|
| BOOLEAN             | bool/boolean    |
| TINYINT             | int8            |
| SMALLINT            | int16           |
| INT                 | int32           |
| BIGINT              | int64           |
| FLOAT               | float16         |
| DOUBLE              | float32         |
| BYTES               | binary          |
| DATE                | DATE            |
| TIME                | TIME            |
| TIMESTAMP           | TIMESTAMP       |
| STRING              | string/utf8     |


## Task Example

### Simple

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"

  # You can set spark configuration here
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    plugin_output = "fake"
  }
}

transform {
}

sink {
  Lance {
    dataset_path = "/tmp/seatunnel_mnt/lanceTest/lance_sink_table"
    namespace_type = "dir"
    namespace_id = "root"
    table = "lance_sink_table"
  }
}

```

## Changelog

<ChangeLog />
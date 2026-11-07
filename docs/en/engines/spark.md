# SeaTunnel With Spark

Apache Spark is the right choice when your team already runs Spark and wants SeaTunnel jobs to fit into that batch or mixed workload environment. If you are evaluating SeaTunnel from scratch and do not need Spark specifically, start with [SeaTunnel Engine](./zeta/about.md) first.

## Start Here

Use this path if you want to run SeaTunnel on Spark:

- [Engine Overview](./overview.md)
- [Quick Start With Spark](../getting-started/locally/quick-start-spark.md)
- [Job Configuration Guide](../getting-started/job-configuration-guide.md)

## When To Choose Spark

Spark is usually the right engine when:

- your organization already runs Spark clusters in production
- the surrounding workloads are mainly batch-oriented
- you want SeaTunnel to align with an existing Spark ecosystem and deployment model

## Spark-Specific Configuration

Spark-specific job options live in the `env` block and use the `spark.` prefix.

Example:

```hocon
env {
  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory = "2g"
  spark.executor.instances = "2"
  spark.yarn.priority = "100"
  spark.dynamicAllocation.enabled = "false"
}
```

## Command Line Example

Spark on YARN cluster mode:

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode cluster --config config/example.conf
```

Spark on YARN client mode:

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

## Minimal Example Job

The example below runs on Spark and prints generated records to the console.

```hocon
env {
  parallelism = 1

  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory = "2g"
  spark.executor.instances = "1"
  spark.yarn.priority = "100"
  spark.dynamicAllocation.enabled = "false"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

If you need more transform options, see [Transforms Catalog](../transforms) and [Transform Common Options](../transforms/common-options/common-options.md).

## Running From A Source Checkout

If you are running examples from the repository source tree, the example module is:

- `seatunnel-examples/seatunnel-spark-connector-v2-example`

The example entry point is:

- `org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample`

## Nested Arrays

For fields whose SeaTunnel type is an array, including types provided by connectors or catalogs, the Spark translation layer preserves array values across source, transform and sink conversion boundaries, including arrays containing arrays, maps, rows and decimals. For example, an `ARRAY<ARRAY<INT>>` field retains its inner arrays, including empty arrays.

Configuration `schema` strings accept array elements of string, boolean, integer, floating-point, map and array types, including `array<array<map<string,array<int>>>>`. Other element types, such as row, decimal, date, time, timestamp and bytes, require types supplied by a connector or catalog.

Connector materialization remains separate from Spark conversion. JSON-based sources such as FakeSource cannot currently materialize parser-declared arrays of arrays of maps, even though the schema string parses successfully.

Null elements, including null maps and rows, remain null. Arrays returned to SeaTunnel transforms use the declared element class (for example, `String[]` or `Integer[]`), and empty arrays retain their type at transform and sink boundaries. See [Spark Array Conversion](../introduction/concepts/incompatible-changes.md#spark-array-conversion) for migration guidance.

Every array must have one compatible element type. Mixed element shapes, such as an integer and an array in the same array, are not supported. Nested SQL expressions and map-of-map pipelines remain subject to the transform and connector type limits. Spark array schemas do not carry the field metadata needed to infer SeaTunnel `TIME` or `TIMESTAMP_TZ`: their physical `BIGINT` and `DECIMAL` representations do not identify the logical time type. Reverse schema inference for `TIMESTAMP` arrays is also unsupported.

## Next Steps

- [Quick Start With Spark](../getting-started/locally/quick-start-spark.md)
- [Spark Translation Layer](../architecture/api-design/spark-translation-layer.md)
- [Transforms Catalog](../transforms)
- [SeaTunnel Engine](./zeta/about.md) if you want to compare against the default engine

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

Spark 4.1 on YARN client mode (JDK 17+, from `-spark41-bin` tarball):

```shell
./bin/start-seatunnel-spark-4.1-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

## Spark 4.1 Distribution

Spark 4.1 requires **JDK 17 or later** and is shipped in a separate binary release:

- `apache-seatunnel-${version}-bin.tar.gz` — Spark 2.4 / 3.3, Flink, and SeaTunnel Engine (JDK 8+)
- `apache-seatunnel-${version}-spark41-bin.tar.gz` — Spark 4.1 starter and a minimal connector set (JDK 17+)

The Spark 4.1 tarball currently bundles starter logging dependencies, `connector-fake`, `connector-console`, `connector-assert`, Scala 2.13 runtime libraries (`scala-library`, `scala-reflect`), and common JDBC/Hadoop optional libraries. Install additional connectors with `bin/install-plugin.sh` as needed.

### Spark 4.1 support scope

The first Spark 4.1 release slice focuses on a **minimal source → sink path**. It does **not** ship `seatunnel-transforms-v2` in the `-spark41-bin` tarball, because transform plugins are still built against Scala 2.12 and conflict with Spark 4.1's Scala 2.13 runtime.

- Supported now: source and sink connectors on Spark 4.1 (for example `FakeSource` → `Console` / `Assert`)
- Not supported yet in the Spark 4.1 tarball: transform stages such as `FieldMapper`, `Sql`, and other `seatunnel-transforms-v2` plugins
- Follow-up work is tracked in [design issue #11184](https://github.com/apache/seatunnel/issues/11184)

If your job includes a `transform { ... }` block, use the standard `-bin` tarball with Spark 2.4 / 3.3, or SeaTunnel Engine, until Spark 4.1 transform support lands.

## Minimal Example Job (Spark 2.4 / 3.3)

The example below runs on Spark 2.4 / 3.3 and prints generated records to the console. It includes a `FieldMapper` transform.

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

## Minimal Spark 4.1 Example Job

Use this transform-free job with the `-spark41-bin` tarball. It matches the current Spark 4.1 E2E smoke test (`FakeSource` → `Assert`):

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"

  spark.app.name = "spark41-example"
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

sink {
  Assert {
    plugin_input = "fake"
    rules = {
      field_rules = [
        {
          field_name = name
          field_type = string
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        }
      ]
    }
  }
}
```

## Running From A Source Checkout

If you are running examples from the repository source tree, the example module is:

- `seatunnel-examples/seatunnel-spark-connector-v2-example`

The example entry point is:

- `org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample`

## Next Steps

- [Quick Start With Spark](../getting-started/locally/quick-start-spark.md)
- [Spark Translation Layer](../architecture/api-design/spark-translation-layer.md)
- [Transforms Catalog](../transforms)
- [SeaTunnel Engine](./zeta/about.md) if you want to compare against the default engine

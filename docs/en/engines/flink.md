# SeaTunnel With Flink

Apache Flink is a strong choice when your team already operates Flink clusters and wants SeaTunnel jobs to reuse that runtime platform. If you are evaluating SeaTunnel for the first time and do not have existing Flink operations, start with [SeaTunnel Engine](./zeta/about.md) first and come back here only when Flink is the real requirement.

## Start Here

Use this path if you want to run SeaTunnel on Flink:

- [Engine Overview](./overview.md)
- [Quick Start With Flink](../getting-started/locally/quick-start-flink.md)
- [Job Configuration Guide](../getting-started/job-configuration-guide.md)

## When To Choose Flink

Flink is usually the right engine when:

- your organization already runs Flink clusters in production
- you want to reuse existing Flink deployment, monitoring, and operational practices
- your jobs need to align with a broader Flink-based stream processing environment

## Flink-Specific Configuration

SeaTunnel job-level Flink configuration uses the `flink.` prefix inside the `env` block.

Example:

```hocon
env {
  parallelism = 1
  flink.execution.checkpointing.unaligned.enabled = true
}
```

Inline enumeration types are not fully supported in the SeaTunnel job config. For settings that require enum-like values outside the supported inline types, configure them in Flink itself. The common inline-supported value types are:

- `Integer`
- `Boolean`
- `String`
- `Duration`

## Minimal Example Job

The example below runs on Flink and prints generated records to the console.

```hocon
env {
  parallelism = 1
  checkpoint.interval = 5000

  flink.execution.checkpointing.mode = "EXACTLY_ONCE"
  flink.execution.checkpointing.timeout = 600000
}

source {
  FakeSource {
    row.num = 16
    plugin_output = "fake_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(33, 18)"
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake_table"
    plugin_output = "fake_output"
    field_mapper = {
      c_string = c_string
      c_int = c_int
    }
  }
}

sink {
  Console {
    plugin_input = "fake_output"
  }
}
```

If you need more transform options, see [Transforms Catalog](../transforms) and [Transform Common Options](../transforms/common-options/common-options.md).

## Running From A Source Checkout

If you are running examples from the repository source tree, the example module is:

- `seatunnel-examples/seatunnel-flink-connector-v2-example`

The example entry point is:

- `org.apache.seatunnel.example.flink.v2.SeaTunnelApiExample`

## Next Steps

- [Quick Start With Flink](../getting-started/locally/quick-start-flink.md)
- [Flink Translation Layer](../architecture/api-design/flink-translation-layer.md)
- [Transforms Catalog](../transforms)
- [SeaTunnel Engine](./zeta/about.md) if you want to compare against the default engine

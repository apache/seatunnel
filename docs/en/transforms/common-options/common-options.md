---
sidebar_position: 1
title: Transform Common Options
---

# Transform Common Options

SeaTunnel transforms share a small set of wiring options. These options do not define the transform logic itself. They define how a transform connects to upstream and downstream datasets inside a job.

## Deprecated Option Names

:::caution Warning

The old option names `source_table_name` and `result_table_name` are deprecated. Use `plugin_input` and `plugin_output` in new configurations.

:::

## Shared Wiring Options

| Option | Meaning | Typical use |
| --- | --- | --- |
| `plugin_input` | Declares which upstream dataset the current transform consumes. If it is omitted, the transform reads from the previous plugin in the configuration order. | Use it when reading from a named intermediate dataset or when the job is not a simple linear chain. |
| `plugin_output` | Registers the current transform result as a named dataset that later transforms or sinks can reference. | Use it when multiple downstream steps need the same result, or when you want the pipeline graph to be explicit. |

## How Dataset Wiring Works

At a high level, transform wiring works in two modes:

- **implicit chaining**: each plugin reads from the previous plugin output in configuration order
- **explicit dataset wiring**: plugins reference named datasets through `plugin_input` and `plugin_output`

Implicit chaining is shorter for very small jobs. Explicit naming is better when:

- one source feeds multiple downstream steps
- a transform result is reused by more than one sink
- the job contains multiple logical tables
- you want the pipeline graph to be easier to read and debug

## Example: Named Dataset Flow

The example below shows a source registering `fake`, a transform reading that dataset and producing `fake1`, and two sinks consuming different outputs.

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        c_timestamp = "timestamp"
      }
    }
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, upper(name) as name, age + 1 as age, c_timestamp from fake"
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
  Console {
    plugin_input = "fake"
  }
}
```

## Practical Guidelines

- keep dataset names short and descriptive
- prefer explicit naming once a job has branching or multi-table behavior
- keep transform docs and config examples aligned with the real dataset names
- avoid using dataset wiring to hide overly complex logic; split the job when it becomes hard to follow

## Related Docs

- [Transform Plugin System](../../architecture/transform-plugin-system.md)
- [Job Configuration Guide](../../getting-started/job-configuration-guide.md)
- [Transforms Catalog](..)

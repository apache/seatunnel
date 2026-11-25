---
sidebar_position: 4
---

# Sink Common Options

> Common parameters of sink connectors

For sink write mode selection, automatic SQL generation, `schema_save_mode`, `data_save_mode`,
`custom_sql`, and `enable_upsert`, see [Sink Write Modes and Save Modes](sink-write-modes.md).

:::caution warn

The old configuration name `source_table_name` is deprecated, please migrate to the new name `plugin_input` as soon as possible.

:::

| Name          | Type   | Required | Default | Description                                                                                                                                                                                                                                                                                                                                  |
|---------------|--------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| plugin_input  | list   | No       | -       | When `plugin_input` is not specified, the plug-in looks up the upstream dataset by the default dataset identifier `DEFAULT_ID`, i.e. it binds to the dataset produced by an upstream source or transform that does not declare `plugin_output`; if no such dataset exists, the Zeta engine falls back to the last dataset produced in the job (tolerating simple jobs). <br/> When `plugin_input` is specified, the current plug-in processes the data set corresponding to this parameter. The value can be a single upstream dataset name or an array of names; with several names the Zeta engine unions those upstream data sets into this sink. |
| metadata_datasource_id | String | No       | -       | The data source ID for retrieving connection configuration from Metadata Center. When specified, the connector will fetch connection details (e.g., URL, username, password) from the external metadata service instead of using direct configuration. See [Metadata SPI](../../introduction/concepts/metadata-spi.md) for more information. |

### plugin_input [list]

When `plugin_input` is not specified, the plug-in looks up the upstream dataset by the default dataset identifier `DEFAULT_ID` instead of by file adjacency. Upstream sources and transforms that do not declare `plugin_output` publish their dataset under `DEFAULT_ID`, so that is the dataset this plug-in consumes.

When `plugin_input` is specified, the current plug-in processes the data set corresponding to this parameter. The value can be a single upstream dataset name (a string) or an array of names.

### metadata_datasource_id [string]

The data source ID for retrieving connection configuration from Metadata Center. When specified, the connector will fetch connection details (e.g., URL, username, password) from the external metadata service instead of using direct configuration. See [Metadata SPI](../../introduction/concepts/metadata-spi.md) for more information.

# Important note

A sink only consumes data sets, so it has no `plugin_output` option. The `plugin_input` value must match the `plugin_output` declared by an upstream source or transform. Binding is resolved by these dataset identifiers, not by the order of plugins in the configuration file: when `plugin_input` is omitted, the plug-in uses the default identifier `DEFAULT_ID`, and every upstream dataset produced without `plugin_output` is published under that same identifier — so several sinks may consume the same default dataset without any explicit input. Only when the job is not a simple graph (more than one source, transform or sink) does validation require every source/transform to declare `plugin_output` and every transform/sink to declare `plugin_input` (see the note at the end of this page).

## Task Example

### Simple

> This is the process of passing a data source through two transforms and returning two different pipelines to different sinks

```bash
source {
    FakeSource {
      parallelism = 2
      plugin_output = "fake"
      schema = {
        fields {
          name = "string"
          age = "int"
        }
      }
    }
}

transform {
    Filter {
      plugin_input = "fake"
      fields = [name]
      plugin_output = "fake_name"
    }
    Filter {
      plugin_input = "fake"
      fields = [age]
      plugin_output = "fake_age"
    }
}

sink {
    Console {
      plugin_input = "fake_name"
    }
    Console {
      plugin_input = "fake_age"
    }
}
```

> If the job has only one source, one (or zero) transform and one sink, you do not need to specify `plugin_input` and `plugin_output`: upstream outputs are published under the default identifier, and a transform/sink without `plugin_input` binds to that default dataset (falling back to the last dataset produced in the job when there is no match).
> If the number of any of sources, transforms or sinks is greater than 1, the job is validated by named datasets: every source/transform must declare `plugin_output` and every transform/sink must declare `plugin_input`. Sinks only consume datasets and have no `plugin_output`.

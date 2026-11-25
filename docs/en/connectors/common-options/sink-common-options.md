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
| plugin_input | list | Required for complex Zeta jobs | - | Upstream dataset name or list of names. |
| metadata_datasource_id | String | No       | -       | The data source ID for retrieving connection configuration from Metadata Center. When specified, the connector will fetch connection details (e.g., URL, username, password) from the external metadata service instead of using direct configuration. See [Metadata SPI](../../introduction/concepts/metadata-spi.md) for more information. |

### plugin_input [list]

Set `plugin_input` to a dataset name (a string) or an array of names declared by upstream sources or transforms using `plugin_output`. Zeta unions compatible inputs when several datasets are selected.

### metadata_datasource_id [string]

The data source ID for retrieving connection configuration from Metadata Center. When specified, the connector will fetch connection details (e.g., URL, username, password) from the external metadata service instead of using direct configuration. See [Metadata SPI](../../introduction/concepts/metadata-spi.md) for more information.

# Important note

For a simple Zeta job with one source, one sink, and at most one transform, omitted `plugin_output` and `plugin_input` use the default dataset identifier `DEFAULT_ID`. If none of the sink's input names resolve, Zeta falls back to the last dataset produced.

If a Zeta job has multiple sources, transforms, or sinks, each source and transform must declare `plugin_output`, and each transform and sink must declare `plugin_input`. To send the same upstream dataset to multiple sinks, set each sink's `plugin_input` to that dataset's output name.

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

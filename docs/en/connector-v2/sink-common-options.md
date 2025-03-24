---
sidebar_position: 4
---

# Sink Common Options

> Common parameters of sink connectors

:::caution warn

The old configuration name `source_table_name` is deprecated, please migrate to the new name `plugin_input` as soon as possible.

:::

| Name         | Type   | Required | Default | Description                                                                                                                                                                                                                                                                |
|--------------|--------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| plugin_input | String | No       | -       | When `plugin_input` is not specified, the current plug-in processes the data set `dataset` output by the previous plugin in the configuration file <br/> When `plugin_input` is specified, the current plug-in is processing the data set corresponding to this parameter. |

# Important note

When the job configuration `plugin_input` you must set the `plugin_output` parameter

## Task Example

### Simple:

> This is the process of passing a data source through two transforms and returning two different pipiles to different sinks

```bash
source {
    FakeSourceStream {
      parallelism = 2
      plugin_output = "fake"
      field_name = "name,age"
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

> If the job only have one source and one(or zero) transform and one sink, You do not need to specify `plugin_input` and `plugin_output` for connector.
> If the number of any operator in source, transform and sink is greater than 1, you must specify the `plugin_input` and `plugin_output` for each connector in the job.


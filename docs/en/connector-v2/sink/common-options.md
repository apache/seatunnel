# Sink Common Options

> Common parameters of sink connectors

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| source_table_name | string | no       | -             |
| parallelism       | int    | no       | -             |

### source_table_name [string]

When `source_table_name` is not specified, the current plug-in processes the data set `dataset` output by the previous plugin in the configuration file;

When `source_table_name` is specified, the current plug-in is processing the data set corresponding to this parameter.

### parallelism [int]

When `parallelism` is not specified, the `parallelism` in env is used by default.

When parallelism is specified, it will override the parallelism in env.

## Examples

```bash
source {
    FakeSourceStream {
      parallelism = 2
      result_table_name = "fake"
      field_name = "name,age"
    }
}

transform {
    Filter {
      source_table_name = "fake"
      fields = [name]
      result_table_name = "fake_name"
    }
    Filter {
      source_table_name = "fake"
      fields = [age]
      result_table_name = "fake_age"
    }
}

sink {
    Console {
      source_table_name = "fake_name"
    }
    Console {
      source_table_name = "fake_age"
    }
}
```

> If the job only have one source and one(or zero) transform and one sink, You do not need to specify `source_table_name` and `result_table_name` for connector.
> If the number of any operator in source, transform and sink is greater than 1, you must specify the `source_table_name` and `result_table_name` for each connector in the job.


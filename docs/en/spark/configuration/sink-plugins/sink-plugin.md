# Common Options

> Sink Common Options [Spark]

## Sink Plugin common parameters

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| source_table_name | string | no       | -             |

### source_table_name [string]

When `source_table_name` is not specified, the current plug-in processes the data set `dataset` output by the previous plugin in the configuration file;

When `source_table_name` is specified, the current plug-in is processing the data set corresponding to this parameter.

## Examples

```bash
stdout {
    source_table_name = "view_table"
}
```

> Output a temporary table named `view_table`.

```bash
stdout {}
```

> If `source_table_name` is not configured, output the processing result of the last `Filter` plugin in the configuration file

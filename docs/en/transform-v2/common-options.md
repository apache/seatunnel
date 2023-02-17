# Transform Common Options

> Common parameters of source connectors

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| result_table_name | string | no       | -             |
| source_table_name | string | no       | -             |

### source_table_name [string]

When `source_table_name` is not specified, the current plug-in processes the data set `(dataset)` output by the previous plug-in in the configuration file;

When `source_table_name` is specified, the current plugin is processing the data set corresponding to this parameter.

### result_table_name [string]

When `result_table_name` is not specified, the data processed by this plugin will not be registered as a data set that can be directly accessed by other plugins, or called a temporary table `(table)`;

When `result_table_name` is specified, the data processed by this plugin will be registered as a data set `(dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` . The dataset registered here can be directly accessed by other plugins by specifying `source_table_name` .

## Examples


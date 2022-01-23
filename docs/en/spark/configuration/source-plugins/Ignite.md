# Source plugin: Ignite

### Description

Read data from Ignite.

### Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [table](#table-string)       | string | yes      | -             |
| [config.file.path](#config.file.path-string)        | string | yes      | -         |

##### table [string]

Ignite table

##### config.file.path [string]

Ignite config file path (just support local file system)

##### options.xxx

Refer to [spark-ignite-options](https://ignite.apache.org/docs/latest/extensions-and-integrations/ignite-for-spark/ignite-dataframe) for configurations.

##### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

### Example

```bash
source {
    ignite {
        table = "City"
        config.file.path = "/tmp/config/example-shared-rdd.xml"
    }
}

```


# Source plugin: Delta

## Description

Read data from Delta Lake.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| delta.read.path | string | yes      | -             |

Refer to [Delta read options](https://docs.delta.io/0.6.0/delta-batch.html#read-a-table) for configurations.

### delta.read.path

Comma separated list of file paths to read within a Delta table.


## Example

```bash
Delta {
    delta.read.path = "hdfs://"
}
```



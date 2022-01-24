# Source plugin: Delta

## Description

Read data from Delta Lake.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| delta.read.path | string | yes      | -             |
| timestampAsOf | string | no      | -             |
| versionAsOf | string | no     | -             |

Refer to [Delta read options](https://docs.delta.io/0.6.0/delta-batch.html#read-a-table) for configurations.

### delta.read.path

Comma separated list of file paths to read within a Delta table.
### timestampAsOf
when we want to query an older snapshot of a table ,we can use `timestampAsOf`.
For timestamp_string, only date or timestamp strings are accepted. For example, "2019-01-01" and "2019-01-01T00:00:00.000Z".

### versionAsOf
when we want to query an older snapshot of a table ,we can use `versionAsOf`.

## Example

```bash
Delta {
    delta.read.path = "hdfs://"
}
```



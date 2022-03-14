# Fake

> Source plugin : Fake [Flink]

## Description

`Fake Source` is mainly used to automatically generate data. The data has only two columns. The first column is of `String type` and the content is a random one from `["Gary", "Ricky Huo", "Kid Xiong"]` . The second column is of `Long type` , which is The current 13-bit timestamp is used as input for functional verification and testing of `seatunnel` .

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| parallelism    | `Int`  | no       | -             |
| common-options |`string`| no       | -             |

### parallelism [`Int`]

The parallelism of an individual operator, for Fake Source

### common options [`string`]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```bash
source {
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }
}
```

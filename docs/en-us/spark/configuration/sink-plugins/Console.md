# Sink plugin : Console [Spark]

## Description

Output data to standard output/terminal, which is often used for debugging, which makes it easy to observe the data.

## Options

| name           | type   | required | default value | engine                |
| -------------- | ------ | -------- | ------------- | --------------------- |
| limit          | number | no       | 100           | batch/spark streaming |
| serializer     | string | no       | plain         | batch/spark streaming |
| common-options | string | no       | -             | all streaming         |

### limit [number]

Limit the number of `rows` to be output, the legal range is `[-1, 2147483647]` , `-1` means that the output is up to `2147483647` rows

### serializer [string]

The format of serialization when outputting. Available serializers include: `json` , `plain`

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Examples

```bash
console {
    limit = 10
    serializer = "json"
}
```

> Output 10 pieces of data in Json format

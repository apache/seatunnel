# Console

> Sink plugin : Console [Flink]

## Description

Used for functional testing and debugging, the results will be output in the stdout tab of taskManager

## Options

| name           | type   | required | default value |
|----------------|--------| -------- |---------------|
| limit          | int    | no       | INT_MAX       |
| common-options | string | no       | -             |

### limit [int]

limit console result lines

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Examples

```bash
ConsoleSink{}
```

## Note

Flink's console output is in flink's WebUI

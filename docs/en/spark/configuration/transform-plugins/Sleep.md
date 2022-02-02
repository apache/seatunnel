# Sleep

> Transform plugin : Sleep [Spark]

## Description

Sleep for a duration, useful when you want to debug some plugins in local mode.

## Options

| name     | type   | required | default value |
| -------- | ------ | -------- | ------------- |
| timeunit | string | no       | SECONDS       |
| timeout  | bigint | yes      | -             |

### timeunit [string]

The TimeUnit used for sleep, value betweens in `NANOSECONDS`, `MICROSECONDS`, `MILLISECONDS`, `SECONDS`, `MINUTES`, `HOURS`, `DAYS`, which is compatible with `java.util.concurrent.TimeUnit`.

### timeout [string]

The minimum time to sleep.

## Examples

Sleep for 5 minutes:

```bash
sleep {
    timeunit = "MINUTES",
    timeout = 5
}
```


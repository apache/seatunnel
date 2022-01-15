# Source plugin : Socket [Flink]

## Description

Socket as data source

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| host           | string | no       | localhost     |
| port           | int    | no       | 9999          |
| common-options | string | no       | -             |

### host [string]

socket server hostname

### port [int]

socket server port

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```bash
source {
  SocketStream{
        result_table_name = "socket"
        field_name = "info"
  }
}
```

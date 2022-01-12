# Source plugin : SocketStream [Spark]

## Description

`SocketStream` is mainly used to receive `Socket` data and is used to quickly verify `Spark streaming` computing.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| host           | string | no       | localhost     |
| port           | number | no       | 9999          |
| common-options | string | yes      | -             |

### host [string]

socket server hostname

### port [number]

socket server port

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```bash
socketStream {
  port = 9999
}
```

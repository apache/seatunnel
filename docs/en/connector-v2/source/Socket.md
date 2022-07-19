# Socket
## Description

Used to read data from Socket. Both support streaming and batch mode.

##  Options

| name | type   | required | default value |
| --- |--------| --- | --- |
| host | String | No | localhost |
| port | Integer | No | 9999 |

### host [string]
socket server host

### port [integer]

socket server port

## Example

simple:

```hocon
Socket {
        host = "localhost"
        port = 9999
    }
```


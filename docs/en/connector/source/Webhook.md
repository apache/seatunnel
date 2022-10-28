# WebhookStream

> Webhook source connector

## Description

Provide http interface to push data，only supports post requests.

:::tip

Engine Supported and plugin name

* [x] Spark: WebhookStream
* [ ] Flink

:::

## Options

| name | type   | required | default value |
| ---- | ------ | -------- | ------------- |
| port | int    | no       | 9999          |
| path | string | no       | /             |

### port[int]

Port for push requests, default 9999.

### path[string]

Push request path, default "/".

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details.

## Example

```
WebhookStream {
      result_table_name = "request_body"
   }
```


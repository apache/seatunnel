# Http

> Http source connector

## Description

Get data from http or https interface

:::tip

Engine Supported and plugin name

* [x] Spark: Http
* [x] Flink: Http

:::

## Options

| name           | type   | required | default vale |
| -------------- | ------ | -------- | ------------ |
| url            | string | yes      | -            |
| method         | string | no       | GET          |
| header         | string | no       |              |
| request_params | string | no       |              |
| sync_path      | string | no       |              |

### url [string]

HTTP request path, starting with http:// or https://.

### method[string]

HTTP request method, GET or POST, default GET.

### header[string]

HTTP request header, json format.

### request_params[string]

HTTP request parameters, json format. Use string with escapes to save json

### sync_path[string]

HTTP multiple requests, the storage path of parameters used for synchronization (hdfs).

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details.

## Example

```bash
 Http {
    url = "http://date.jsontest.com/"
    result_table_name= "response_body"
   }
```

## Notes

According to the processing result of the http call, to determine whether the synchronization parameters need to be updated, it needs to be written to hdfs through the hdfs sink plugin after the judgment is made outside the http source plugin.

# Sink plugin : Elasticsearch [Flink]

## Description

Output data to ElasticSearch

## Options

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| hosts             | array  | yes      | -             |
| index_type        | string | no       | log           |
| index_time_format | string | no       | yyyy.MM.dd    |
| index             | string | no       | seatunnel     |
| common-options    | string | no       | -             |

### hosts [array]

`Elasticsearch` cluster address, the format is `host:port` , allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]` .

### index_type [string]

Elasticsearch index type

### index_time_format [string]

When the format in the `index` parameter is `xxxx-${now}` , `index_time_format` can specify the time format of the `index` name, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol | Description        |
| ------ | ------------------ |
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

See [Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html) for detailed time format syntax.

### index [string]

Elasticsearch `index` name. If you need to generate an `index` based on time, you can specify a time variable, such as `seatunnel-${now}` . `now` represents the current data processing time.

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Examples

```bash
elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel"
}
```

> Write the result to the index of the `Elasticsearch` cluster named `seatunnel`

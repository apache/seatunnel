# Sink plugin : Elasticsearch [Flink]

## 描述

将数据输出到 Elasticsearch

## Options

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| hosts             | array  | yes      | -             |
| index_type        | string | no       | log           |
| index_time_format | string | no       | yyyy.MM.dd    |
| index             | string | no       | seatunnel     |
| common-options    | string | no       | -             |
| parallelism       | int    | no       | -             |

### hosts [array]

`Elasticsearch` 集群地址，格式为 `host:port` 允许指定多个主机。例如：`["host1:9200", "host2:9200"]`

### index_type [string]

Elasticsearch 索引类型

### index_time_format [string]

当 `index` 参数中的格式为 `xxxx-${now}` ， `index_time_format` 可以指定名称的时间格式 `index` 默认时间格式为 `yyyy.MM.dd` 。常用时间格式如下所示：

| Symbol | Description        |
| ------ | ------------------ |
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

有关时间格式参考： [Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html)

### index [string]

Elasticsearch `index` 名称。 如果需要 `index` 基于时间生成可以指定时间变量，例如 `seatunnel-${now}` . `now` 表示当前数据处理时间。

### parallelism [`Int`]

单个算子、数据源以及数据接收器的并行性

### common options [string]

Sink 插件常用参数 [Sink Plugin](./sink-plugin.md)

## Examples

```bash
elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel"
}
```

> 将结果写入 `Elasticsearch` 的集群索引中 `seatunnel`

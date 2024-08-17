# Console

> Console 数据接收器

## 支持连接器版本

- 所有版本

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

接收Source端传入的数据并打印到控制台。支持批同步和流同步两种模式。

> 例如，来自上游的数据为 [`age: 12, name: jared`] ，则发送到控制台的内容为: `{"name":"jared"，"age":17}`

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 接收器选项

|         名称         |   类型    | 是否必须 | 默认值 |                            描述                             |
|--------------------|---------|------|-----|-----------------------------------------------------------|
| common-options     |         | 否    | -   | Sink插件常用参数，请参考 [Sink常用选项](../sink-common-options.md) 了解详情 |
| log.print.data     | boolean | 否    | -   | 确定是否应在日志中打印数据的标志。默认值为`true`                               |
| log.print.delay.ms | int     | 否    | -   | 将每个数据项打印到日志之间的延迟(以毫秒为单位)。默认值为`0`                          |

## 任务示例

### 简单示例:

> 随机生成的数据,包含两个字段，即 `name`（字符串类型）和 `age`（整型），写入控制台，并行度为 `1`

```
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    result_table_name = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
    source_table_name = "fake"
  }
}
```

### 多数据源示例：

> 多数据源示例，通过配置可以指定数据源写入指定接收器

```
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    result_table_name = "fake1"
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        sex = "string"
      }
    }
  }
   FakeSource {
    result_table_name = "fake2"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
    source_table_name = "fake1"
  }
  Console {
    source_table_name = "fake2"
  }
}
```

## 控制台示例数据

控制台打印的输出:

```
2022-12-19 11:01:45,417 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=1: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CpiOd, 8520946
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=2: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: eQqTs, 1256802974
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=3: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: UsRgO, 2053193072
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=4: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jDQJj, 1993016602
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=5: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: rqdKp, 1392682764
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=6: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: wCoWN, 986999925
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=7: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: qomTU, 72775247
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=8: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jcqXR, 1074529204
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=9: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: AkWIO, 1961723427
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=10: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: hBoib, 929089763
```


import ChangeLog from '../changelog/connector-console.md';

# Console

> Console 数据接收器

## 支持连接器版本

- 所有版本

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

接收 Source 端传入的数据，并打印到 SeaTunnel 任务日志中。Console 支持批处理和流处理，主要用于调试、本地验证和示例任务，不适合作为生产环境的持久化存储。

对于每一条非空数据，Console 会打印子任务编号、行编号、表 ID、行类型和字段值。数组、Map、嵌套行等复杂类型会先转换为易读的字符串再打印。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

Console 可以接收多个上游表的数据，也可以处理 Schema 变更事件并用于日志展示。由于它写入的是日志，不向外部系统持久化数据，因此不提供精确一次写入语义，也不属于真正的 CDC 写入目标。

> Console sink 默认启用 schema 演进处理（`ADD_COLUMN`、`DROP_COLUMN`、`RENAME_COLUMN`、`UPDATE_COLUMN`），上游 schema 的变化会反映在打印出的行类型中。

## 接收器选项

| 名称                       | 类型      | 是否必须 | 默认值  | 描述                                                                 |
|--------------------------|---------|------|------|--------------------------------------------------------------------|
| common-options           |         | 否    | -    | Sink 插件通用参数，详情请参考 [Sink 常用选项](../common-options/sink-common-options.md)。 |
| log.print.data           | boolean | 否    | true | 是否将行数据打印到任务日志。如果只想保留 Console 节点但不打印每行数据，可以设置为 `false`。       |
| log.print.delay.ms       | int     | 否    | 0    | 每处理一行后的非负等待时间，单位为毫秒。调试时可以用它放慢打印速度。                           |
| multi_table_sink_replica | int     | 否    | 1    | 多表写入时每张表对应的 Sink Writer 副本数。                                  |

## 输出格式

Console 会在 Writer 启动时先打印行类型，然后按如下格式打印每行数据：

```text
subtaskIndex=<子任务编号>  rowIndex=<行编号>:  SeaTunnelRow#tableId=<表 ID> SeaTunnelRow#kind=<行类型> : <字段1>, <字段2>, ...
```

- `subtaskIndex` 表示打印该行的 Sink 子任务。
- `rowIndex` 是每个 Sink Writer 内部独立递增的行编号。
- `tableId` 表示上游表标识。单表任务通常显示为 `-1`。
- `row-kind` 表示行变更类型，例如 `INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER` 或 `DELETE`。

## 任务示例

### 简单示例

下面的示例生成 3 行数据，并打印到任务日志。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 3
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
    plugin_input = "fake"
    log.print.data = true
    log.print.delay.ms = 0
  }
}
```

### 多数据源示例

通过 `plugin_input` 可以把不同上游数据分别写入不同的 Console Sink。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake1"
    row.num = 3
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
    plugin_output = "fake2"
    row.num = 3
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
    plugin_input = "fake1"
  }
  Console {
    plugin_input = "fake2"
  }
}
```

### 多表输入示例

当上游 Source 产生多张表时，Console 可以在一个 Sink 中打印这些表的数据。日志中的表 ID 可以帮助区分每行数据来自哪张表。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        row.num = 2
        schema = {
          table = "test.table1"
          columns = [
            { name = id, type = bigint }
            { name = name, type = string }
          ]
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.table2"
          columns = [
            { name = id, type = bigint }
            { name = age, type = int }
          ]
        }
      }
    ]
  }
}

sink {
  Console {
    multi_table_sink_replica = 1
  }
}
```

## 控制台示例数据

控制台打印的输出:

```text
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

## 变更日志

<ChangeLog />

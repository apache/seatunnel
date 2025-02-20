# Druid

> Druid 接收器连接器

## 描述

一个使用向 Druid 发送消息的接收器插件

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [支持多表写入](../../concept/connector-v2-features.md)

## 数据类型映射

| SeaTunnel 数据类型 | Druid 数据类型 |
|----------------|-----------------|
| TINYINT        | LONG            |
| SMALLINT       | LONG            |
| INT            | LONG            |
| BIGINT         | LONG            |
| FLOAT          | FLOAT           |
| DOUBLE         | DOUBLE          |
| DECIMAL        | DOUBLE          |
| STRING         | STRING          |
| BOOLEAN        | STRING          |
| TIMESTAMP      | STRING          |

## 选项

|      名称           |  类型  | 必需 | 默认值 |
|----------------|--------|----|---------------|
| coordinatorUrl | string | 是  | -             |
| datasource     | string | 是  | -             |
| batchSize      | int    | 否  | 10000         |
| common-options |        | 否 | -             |

### coordinatorUrl [string]

Druid的协调器URL主机和端口，示例: "myHost:8888"

### datasource [string]

要写入的数据源名称，示例: "seatunnel"

### batchSize [int]

每批刷新为Druid的行数。默认值为 `1024`.

### common options

Sink插件常用参数，详见 [Sink Common Options](../sink-common-options.md) for details

## 示例

简单的例子:

```hocon
sink {
  Druid {
    coordinatorUrl = "testHost:8888"
    datasource = "seatunnel"
  }
}
```

使用占位符获取上游表元数据示例:

```hocon
sink {
  Druid {
    coordinatorUrl = "testHost:8888"
    datasource = "${table_name}_test"
  }
}
```

## 变更日志

### 下一个版本

- 添加 Druid 接收器连接器


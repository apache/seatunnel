import ChangeLog from '../changelog/connector-blackhole.md';


# BlackHole

> BlackHole sink 连接器

## 描述

一个会丢弃所有接收到的记录的 sink 连接器。这在性能测试或当你想忽略某些数据时非常有用。

## 主要特性

* 丢弃所有数据

## 配置选项

不需要任何配置选项。

## 示例

简单示例，这个示例定义了一个 SeaTunnel 同步任务，从 FakeSource 读取数据并使用 BlackHole sink 丢弃它。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Tom", 25]
      }
    ]
  }
}

sink {
  BlackHole {}
}
```

## Changelog

<ChangeLog />

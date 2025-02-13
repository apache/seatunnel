# Socket

> Socket 接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 描述

用于向Socket Server发送数据。两者都支持流媒体和批处理模式。

> 例如，如果来自上游的数据是[`age:12，name:jared`]，则发送到套接字服务器的内容如下：`{“name”：“jared”，“age”：17}`

## Sink 选项

|      名称      |  类型   | 需要 | 默认  |                                                   描述                                                   |
|----------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------------------|
| host           | String  | 是      |         | socket server host                                                                                              |
| port           | Integer | 是      |         | socket server port                                                                                              |
| max_retries    | Integer | 否       | 3       | The number of retries to send record failed                                                                     |
| common-options |         | 否       | -       | Source plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details |

## 任务示例

> 这是写入Socket端的随机生成数据

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Socket {
    host = "localhost"
    port = 9999
  }
}
```

* 启动端口侦听

```shell
nc -l -v 9999
```

* Start a SeaTunnel task

* Socket Server Console print data

```text
{"name":"jared","age":17}
```

## Changelog

### 2.2.0-beta 2022-09-26

- 添加插座连接器


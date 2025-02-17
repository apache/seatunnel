# Socket

> Socket 数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 描述

用于向Socket Server发送数据。两者都支持流媒体和批处理模式。

> 例如，如果来自上游的数据是[`age:12，name:jared`]，则发送到Socket服务器的内容如下：`{"name"："jared"，"age"：17}`

## Sink 选项

|      名称      |  类型   | 是否必传 | 默认值  |                                                   描述                                                   |
|----------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------------------|
| host           | String  | 是      |         | socket 服务器主机                                                                                              |
| port           | Integer | 是      |         | socket 服务器端口                                                                                              |
| max_retries    | Integer | 否       | 3       | 发送记录的重试失败次数                                                                     |
| common-options |         | 否       | -       | 源插件常用参数，详见[Source common Options]（../sink common-Options.md） |

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

* 启动SeaTunnel任务

* Socket 服务器控制台打印数据

```text
{"name":"jared","age":17}
```

## 更改日志

### 2.2.0-beta 2022-09-26

- 添加Socket 数据接收器


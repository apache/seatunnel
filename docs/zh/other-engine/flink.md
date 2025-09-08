# Flink引擎方式运行SeaTunnel

Flink是一个强大的高性能分布式流处理引擎。你可以搜索 `Apache Flink`获取更多关于它的信息。

### 在Job中设置Flink的配置信息

以 `flink.` 开始：

例子: 我对这个项目设置一个精确的检查点

```
env {
  parallelism = 1  
  flink.execution.checkpointing.unaligned.enabled=true
}
```

枚举类型当前还不支持，你需要在Flink的配置文件中指定它们。暂时只有这些类型的设置受支持：<br/>
Integer/Boolean/String/Duration

### 如何设置一个简单的Flink Job

这是一个运行在Flink中随机生成数据打印到控制台的简单job

```
env {
  # 公共参数
  parallelism = 1
  checkpoint.interval = 5000

  # flink特殊参数
  flink.execution.checkpointing.mode = "EXACTLY_ONCE"
  flink.execution.checkpointing.timeout = 600000
}

source {
  FakeSource {
    row.num = 16
    plugin_output = "fake_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(33, 18)"
        c_timestamp = timestamp
        c_row = {
          c_map = "map<string, string>"
          c_array = "array<int>"
          c_string = string
          c_boolean = boolean
          c_int = int
          c_bigint = bigint
          c_double = double
          c_bytes = bytes
          c_date = date
          c_decimal = "decimal(33, 18)"
          c_timestamp = timestamp
        }
      }
    }
  }
}

transform {
  # 如果你想知道更多关于如何配置seatunnel的信息和查看完整的transform插件，
  # 请访问：https://seatunnel.apache.org/docs/transform-v2/sql
}

sink{
   Console{}   
}
```

### 如何在项目中运行Job

当你将代码拉到本地后，转到 `seatunnel-examples/seatunnel-flink-connector-v2-example` 模块，查找 `org.apache.seatunnel.example.flink.v2.SeaTunnelApiExample` 即可完成job的操作。

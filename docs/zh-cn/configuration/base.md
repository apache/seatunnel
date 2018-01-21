# 通用配置

TODO: 解释event, raw_message, __root__, spark 配置。

一个完整的Waterdrop配置包含`spark`, `input`, `filter`, `output`, 即：

```
spark {
    ...
}

input {
    ...
}

filter {
    ...
}

output {
    ...
}

```


---

一个示例如下：

```
spark {
  # Waterdrop defined streaming batch duration in seconds
  spark.streaming.batchDuration = 5

  spark.app.name = "Waterdrop"
  spark.ui.port = 13000
}

input {
  socket {}
}

filter {
  split {
    fields = ["msg", "name"]
    delimiter = ","
  }
}

output {
  stdout {}
}
```
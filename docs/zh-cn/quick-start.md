# 快速开始

> Requirements:
>   Spark client

Step1: 下载 waterdrop

Step2: 配置 waterdrop

config/waterdrop-env.sh

Step3: 配置application.conf

config/applicaiton.conf

```
spark {
  # Waterdrop defined streaming batch duration in seconds
  spark.streaming.batchDuration = 5

  # see available properties defined by spark: https://spark.apache.org/docs/latest/configuration.html#available-properties
  spark.app.name = "Waterdrop-1"
  spark.ui.port = 13000
}

input {
  socket {}
}
filter {
}

output {
  stdout {}
}

```

Step 4: 启动netcat server用于发送数据

```
nc -l -p 9999
```


Step 5:

启动Waterdrop 接收程序：

```
./bin/start-waterdrop.sh

```

Step 6: 在nc端输入

```
Hello World
```

Step 7: Waterdrop日志打印出

```
+-----------+
|raw_message|
+-----------+
|Hello World|
+-----------+

```

> 无需任何代码、编译、打包，比官方的[Quick Example](https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example)更简单

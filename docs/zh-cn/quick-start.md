# 快速开始

> 我们以一个通过socket接收数据，将数据分割为多个字段，并输出处理结果的应用为例，快速展示Waterdrop的使用方法。

### Step 1: 准备Spark 运行环境

> 如果你熟悉Spark或者已准备好Spark运行环境，可忽略此步骤，Spark不需要做任何特殊配置。

请先[下载Spark](http://spark.apache.org/downloads.html), Spark版本请选择 >= 2.x.x。下载解压后，不需要做任何配置即可提交Spark deploy-mode = local模式的任务。
如果你期望任务运行在Standalone集群或者Yarn、Mesos集群上，请参考Spark官网配置文档。

### Step 2: 下载 Waterdrop(以Waterdrop-1.0.2为例)

下载[Waterdrop安装包](https://github.com/InterestingLab/waterdrop/releases) 并解压:

```
wget https://github.com/InterestingLab/waterdrop/releases/download/v1.0.0/waterdrop-1.0.2.zip -O waterdrop-1.0.2.zip
unzip waterdrop-1.0.2.zip
ln -s waterdrop-1.0.2 waterdrop
```

### Step 3: 配置 Waterdrop

编辑 `config/waterdrop-env.sh`, 指定必须环境配置如SPARK_HOME(Step 1 中Spark下载并解压后的目录)

编辑 `config/application.conf`, 它决定了Waterdrop启动后，数据输入，处理，输出的方式和逻辑。

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

### Step 4: 启动netcat server用于发送数据

```
nc -l -p 9999
```


### Step 5: 启动Waterdrop

```
cd waterdrop
./bin/start-waterdrop.sh --master local[4] --deploy-mode client --config ./config/application.conf

```

### Step 6: 在nc端输入

```
Hello World, Gary
```
Waterdrop日志打印出:

```
+-----------------+-----------+----+
|raw_message      |msg        |name|
+-----------------+-----------+----+
|Hello World, Gary|Hello World|Gary|
+-----------------+-----------+----+
```


### 总结

Waterdrop简单易用，还有更丰富的数据处理功能等待被发现。本文展示的数据处理案例，
无需任何代码、编译、打包，比官方的[Quick Example](https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example)更简单。

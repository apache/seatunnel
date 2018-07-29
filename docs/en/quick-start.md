# Quick Start


> We will show how to use Waterdrop by taking an application that receives data from a socket, splits the data into multiple fields, and outputs the processed result.

### Step 1: Prepare Spark Runtime

> If you are familiar with Spark or you have prepared the Spark runtime environment, you can ignore this step. You do not need do any special configuration in Waterdrop.

Please [download Spark](http://spark.apache.org/downloads.html) firstly. Require Spark version 2.0.0 or later. After download and extract, you can submit Spark application on client mode without any Spark configuration. If you want to launching applications on a cluster, please refer to the Spark configuration document.


### Step 2: Download Waterdrop

下载[Waterdrop安装包](https://github.com/InterestingLab/waterdrop/releases) 并解压:

```
# 以waterdrop 1.0.2为例:
wget https://github.com/InterestingLab/waterdrop/releases/download/v1.0.2/waterdrop-1.0.2.zip -O waterdrop-1.0.2.zip
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

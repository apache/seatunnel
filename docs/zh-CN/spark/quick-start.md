# 快速开始

> 通过一个从`socket`接收数据，然后划分数据到多列后输出的例子来了解如何使用`Seatunnel`.

## 第一步: 准备Spark运行时环境

> 如果你熟悉Spark，或者已经有Spark环境, 可以忽略这一步。我们不需要对Spark做任何配置。

请先[下载Spark](https://spark.apache.org/downloads.html), 选择`Spark版本 >= 2.x.x`。在下载并解压之后, 不需要修改配置就可以指定 `deploy-mode = local`去提交任务。如果想将任务运行在`Standalone clusters`， `Yarn clusters`， `Mesos clusters`, 请参考在Spark官网 [Spark部署文档](https://spark.apache.org/docs/latest/cluster-overview.html).

### 第二布: 下载 Seatunnel

通过[Seatunnel安装包下载](https://github.com/apache/incubator-seatunnel/releases) 下载最新版本 `seatunnel-<version>.zip`

或者下载指定的版本 (以`2.0.4`为例):

```bash
wget https://github.com/apache/incubator-seatunnel/releases/download/v2.0.4/waterdrop-dist-2.0.4-2.11.8-release.zip -O seatunnel-2.0.4.zip
```

下载完成后解压:

```bash
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

## 步骤 3: 配置 seatunnel

- 编辑 `config/seatunnel-env.sh` , 指定必要的环境变量，比如`SPARK_HOME` (步骤 1解压后的目录)

- 创建一个新的 `config/application.conf` , 它决定了`Seatunnel`被启动后，数据如何被输入，处理和输出。

```bash
env {
  # seatunnel defined streaming batch duration in seconds
  spark.streaming.batchDuration = 5

  spark.app.name = "seatunnel"
  spark.ui.port = 13000
}

source {
  socketStream {}
}

transform {
  split {
    fields = ["msg", "name"]
    delimiter = ","
  }
}

sink {
  console {}
}
```

## 步骤 4: 启动 `netcat server`发送数据

```bash
nc -lk 9999
```

## 步骤 5: 启动 Seatunnel

```bash
cd seatunnel
./bin/start-seatunnel-spark.sh \
--master local[4] \
--deploy-mode client \
--config ./config/application.conf
```

## 步骤 6: Input at the `nc` terminal

```bash
Hello World, seatunnel
```

`Seatunnel`日志输出:

```bash
+----------------------+-----------+---------+
|raw_message           |msg        |name     |
+----------------------+-----------+---------+
|Hello World, seatunnel|Hello World|seatunnel|
+----------------------+-----------+---------+
```

## 总结

`Seatunnel`是简单而且易用的, 还有更丰富的数据处理功能有待发现。 本文展示的数据处理案例，无需任何代码、编译、打包，比官方文档更简单 [快速开始](https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example).

如果想了解更多的`Seatunnel`配置案例, 请参考:

- Configuration example 2: [Batch offline batch processing](https://github.com/apache/incubator-seatunnel/blob/dev/config/spark.batch.conf.template)

The above configuration is the default [offline batch configuration template], which can be run directly, the command is as follows:

```bash
cd seatunnel
./bin/start-seatunnel-spark.sh \
--master 'local[2]' \
--deploy-mode client \
--config ./config/spark.batch.conf.template
```

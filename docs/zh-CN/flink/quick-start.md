# 快速开始

> 让我们以一个通过 `socket` 接收数据， 将数据分成多个字段，并输出处理结果的应用程序为例，快速展示如何使用 `seatunnel` 。

## 步骤1：准备好Flink运行环境

> 如果你熟悉`Flink`或者已经准备好了`Flink`的运行环境，你可以忽略这一步。`Flink`不需要任何特殊的配置。

请先 [下载 Flink](https://flink.apache.org/downloads.html), 请选择Flink版本>=1.9.0。下载完成后即可 [安装 Flink](https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/deployment/resource-providers/standalone/overview/)

## 步骤2：下载seatunnel

进入 [seatunnel 安装包](https://github.com/apache/incubator-seatunnel/releases) 下载页面，下载最新版本的 `seatunnel-<version>.zip`

或者直接下载指定的版本（以2.0.4为例）。

```bash
wget https://github.com/apache/incubator-seatunnel/releases/download/v2.0.4/waterdrop-dist-2.0.4-2.11.8-release.zip -O seatunnel-2.0.4.zip
```

下载后，解压：

```bash
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

## 步骤3：配置seatunnel

- 编辑`config/seatunnel-env.sh`，指定必要的环境配置，如`FLINK_HOME`（在步骤1中下载并解压的`Flink`后的目录）。

- 编辑`config/application.conf`，它决定了`seatunnel`启动后数据输入、处理和输出的方式和逻辑。

```bash
env {
  # You can set flink configuration here
  execution.parallelism = 1
  #execution.checkpoint.interval = 10000
  #execution.checkpoint.data-uri = "hdfs://localhost:9000/checkpoint"
}

source {
    SocketStream{
          result_table_name = "fake"
          field_name = "info"
    }
}

transform {
  Split{
    separator = "#"
    fields = ["name","age"]
  }
  sql {
    sql = "select * from (select info,split(info) as info_row from fake) t1"
  }
}

sink {
  ConsoleSink {}
}

```

## 步骤4：启动`netcat服务器`以发送数据

```bash
nc -l -p 9999
```

## 步骤5: 启动 `seatunnel`

```bash
cd seatunnel
./bin/start-seatunnel-flink.sh \
--config ./config/application.conf
```

## 步骤6: 在`nc`终端输入

```bash
xg#1995
```

它被打印在`flink WebUI`的TaskManager Stdout日志中。

```bash
xg#1995,xg,1995
```

## 总结

如果你想了解更多的 `seatunnel` 配置例子，请参考。

- 配置示例1：[流媒体计算](https://github.com/apache/incubator-seatunnel/blob/dev/config/flink.streaming.conf.template)

上面的配置是默认的`[流媒体配置模板]`，可以直接运行，命令如下。:

```bash
cd seatunnel
./bin/start-seatunnel-flink.sh \
--config ./config/flink.streaming.conf.template
```

- 配置示例2：[批处理离线批处理](https://github.com/apache/incubator-seatunnel/blob/dev/config/flink.batch.conf.template)

上面的配置是默认的`[离线批处理配置模板]`，可以直接运行，命令如下:

```bash
cd seatunnel
./bin/start-seatunnel-flink.sh \
--config ./config/flink.batch.conf.template
```
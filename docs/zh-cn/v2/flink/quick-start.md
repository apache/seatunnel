# 快速开始

> 我们以一个通过socket接收数据，将数据分割为多个字段，并输出处理结果的应用为例，快速展示Waterdrop的使用方法。

### Step 1: 准备Flink 运行环境

> 如果你熟悉Flink或者已准备好Flink运行环境，可忽略此步骤，Flink不需要做任何特殊配置。

请先[下载Flink](https://flink.apache.org/downloads.html), Flink版本请选择 >= 1.9.0。下载完成进行[安装](https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/ops/deployment/cluster_setup.html)

### Step 2: 下载 Waterdrop

进入[Waterdrop安装包下载页面](https://github.com/InterestingLab/waterdrop/releases/latest)，下载最新版`Waterdrop-<version>.zip`

或者直接下载指定版本（以2.0.0为例）：

```
wget https://github.com/InterestingLab/waterdrop/releases/download/v2.0.0/waterdrop-2.0.0.zip -O waterdrop-2.0.0.zip
```

下载后，解压：

```
unzip waterdrop-<version>.zip
ln -s waterdrop-<version> waterdrop
```

### Step 3: 配置 Waterdrop

编辑 `config/waterdrop-env.sh`, 指定必须环境配置如FLINK_HOME(Step 1 中Flink下载并解压后的目录)

编辑 `config/application.conf`, 它决定了Waterdrop启动后，数据输入，处理，输出的方式和逻辑。

```
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

### Step 4: 启动netcat server用于发送数据

```
nc -l -p 9999
```


### Step 5: 启动Waterdrop

```
cd waterdrop
./bin/start-waterdrop.sh  --config ./config/application.conf

```

### Step 6: 在nc端输入

```
xg#1995
```
在flink Web-UI的TaskManager Stdout日志打印出:

```
xg#1995,xg,1995
```


### 总结


如果想了解更多的Waterdrop配置示例可参见：

[配置示例1 : Streaming 流式计算](https://github.com/InterestingLab/waterdrop/blob/wd-v2-baseline/config/flink.streaming.conf.template)

以上配置为默认【流式处理配置模版】，可直接运行，命令如下：

```
cd waterdrop
./bin/start-waterdrop.sh --config ./config/flink.streaming.conf.template

```

[配置示例2 : Batch 离线批处理](https://github.com/InterestingLab/waterdrop/blob/wd-v2-baseline/config/flink.batch.conf.template)

以上配置为默认【离线批处理配置模版】，可直接运行，命令如下：

```
cd waterdrop
./bin/start-waterdrop.sh --config ./config/flink.batch.conf.template

```

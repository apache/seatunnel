## 部署和运行

> Seatunnel v2 For Spark 依赖Java运行时环境和Spark. 详细的Seatunnel安装步骤, 请参考 [安装Seatunnel](./installation.md)

下面主要介绍不同的任务运行模式:

## Local模式运行

```bash
./bin/start-seatunnel-spark.sh \
--master local[4] \
--deploy-mode client \
--config ./config/application.conf
```

## Spark Standalone cluster模式运行

```bash
# client mode
./bin/start-seatunnel-spark.sh \
--master spark://ip:7077 \
--deploy-mode client \
--config ./config/application.conf

# cluster mode
./bin/start-seatunnel-spark.sh \
--master spark://ip:7077 \
--deploy-mode cluster \
--config ./config/application.conf
```

## Yarn 模式运行

```bash
# client mode
./bin/start-seatunnel-spark.sh \
--master yarn \
--deploy-mode client \
--config ./config/application.conf

# cluster mode
./bin/start-seatunnel-spark.sh \
--master yarn \
--deploy-mode cluster \
--config ./config/application.conf
```

## Mesos cluster模式运行

```bash
# cluster mode
./bin/start-seatunnel-spark.sh \
--master mesos://ip:7077 \
--deploy-mode cluster \
--config ./config/application.conf
```

`start-seatunnel-spark.sh`中`master` and `deploy-mode`的参数含义 , 请参考: [命令说明](./commands/start-seatunnel-spark.sh.md)

如果想指定`seatunnel`运行时使用的资源或其他 `Spark参数` , 可以在配置文件中通过 `--config`指定 :

```bash
env {
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  ...
}
...
```

`seatunnel`的配置方式，请参考`seatunnel` [公共配置](./configuration)

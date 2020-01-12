# 部署与运行

> Waterdrop v2 For Spark 依赖Java运行环境和Spark，详细的Waterdrop 安装步骤参考[安装Waterdrop](/zh-cn/v2/spark/installation)

下面重点说明不同平台的运行方式:

### 在本地以 local 方式运行 Waterdrop

```
./bin/start-waterdrop-spark.sh --master local[4] --deploy-mode client --config ./config/application.conf
```

### 在 Spark Standalone 集群上运行 Waterdrop

```
# client 模式
./bin/start-waterdrop-spark.sh --master spark://207.184.161.138:7077 --deploy-mode client --config ./config/application.conf

# cluster 模式
./bin/start-waterdrop-spark.sh --master spark://207.184.161.138:7077 --deploy-mode cluster --config ./config/application.conf
```

### 在 Yarn 集群上运行 Waterdrop

```
# client 模式
./bin/start-waterdrop-spark.sh --master yarn --deploy-mode client --config ./config/application.conf

# cluster 模式
./bin/start-waterdrop-spark.sh --master yarn --deploy-mode cluster --config ./config/application.conf
```

### 在 Mesos 上运行 Waterdrop

```
# cluster 模式
./bin/start-waterdrop-spark.sh --master mesos://207.184.161.138:7077 --deploy-mode cluster --config ./config/application.conf
```

---

`start-waterdrop-spark.sh` 的 `master`, `deploy-mode` 参数的含义请参考: [命令使用说明](/zh-cn/v2/spark/commands/start-waterdrop-spark.sh)

如果要指定 Waterdrop 运行时占用的资源大小，或者其他 Spark 参数，可以在 `--config` 指定的配置文件里面指定：

```
env {
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  ...
}
...

```

关于如何配置 Waterdrop, 请见[Waterdrop 通用配置](/zh-cn/v2/spark/configuration)

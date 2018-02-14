# 部署与运行

> Waterdrop 依赖Java运行环境和Spark，详细的Waterdrop 安装步骤参考[安装Waterdrop](/zh-cn/installation)

下面重点说明不同平台的运行方式:

### 在本地以local方式运行Waterdrop

```
./bin/start-waterdrop.sh --master local[4] --deploy-mode client --config ./config/application.conf
```

### 在Spark Standalone集群上运行Waterdrop

```
# client 模式
./bin/start-waterdrop.sh --master spark://207.184.161.138:7077 --deploy-mode client --config ./config/application.conf

# cluster 模式
./bin/start-waterdrop.sh --master spark://207.184.161.138:7077 --deploy-mode cluster --config ./config/application.conf
```

### 在Yarn集群上运行Waterdrop

```
# client 模式
./bin/start-waterdrop.sh --master yarn --deploy-mode client --config ./config/application.conf

# cluster 模式
./bin/start-waterdrop.sh --master yarn --deploy-mode cluster --config ./config/application.conf
```

### 在Mesos上运行Waterdrop

```
# cluster 模式
./bin/start-waterdrop.sh --master mesos://207.184.161.138:7077 --deploy-mode cluster --config ./config/application.conf
```

---

start-waterdrop.sh 的`master`, `deploy-mode`参数的含义与Spark `master`, `deploy-mode`相同，
可参考: [Spark Submitting Applications](http://spark.apache.org/docs/latest/submitting-applications.html)

如果要指定Waterdrop运行时占用的资源大小，或者其他Spark参数，可以在`--config`指定的配置文件里面指定：

```
spark {
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  ...
}
...

```

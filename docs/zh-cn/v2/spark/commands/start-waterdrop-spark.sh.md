## start-waterdrop-spark.sh 使用方法


### 使用说明

```bash
 bin/start-waterdrop-spark.sh -c config-path -m master -e deploy-mode -i city=beijing
```

> 使用 `-c` 或者 `--config`来指定配置文件的路径

> 使用 `-m` 或者 `--master` 来指定集群管理器

> 使用 `-e` 或者 `--deploy-mode` 来指定部署模式

> 使用 `-i` 或者 `--variable` 来指定配置文件中的变量，可以配置多个


### 使用案例

```
# Yarn client 模式
./bin/start-waterdrop-spark.sh --master yarn --deploy-mode client --config ./config/application.conf

# Yarn cluster 模式
./bin/start-waterdrop-spark.sh --master yarn --deploy-mode cluster --config ./config/application.conf
```
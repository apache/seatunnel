# 命令使用说明

> 命令使用说明 [Spark]

## Seatunnel spark 启动命令

```bash
bin/start-seatunnel-spark.sh
```

### 使用说明

```bash
bin/start-seatunnel-spark.sh \
-c config-path \
-m master \
-e deploy-mode \
-i city=beijing
```

- 使用 `-c` or `--config` 指定配置文件的路径

- 使用 `-m` or `--master` 指定集群管理器

- 使用 `-e` or `--deploy-mode` 指定部署模式

- 使用 `-i` or `--variable` 指定配置文件中使用的变量，可以使用多次

#### 用例

```bash
# Yarn client mode
./bin/start-seatunnel-spark.sh \
--master yarn \
--deploy-mode client \
--config ./config/application.conf

# Yarn cluster mode
./bin/start-seatunnel-spark.sh \
--master yarn \
--deploy-mode cluster \
--config ./config/application.conf
```

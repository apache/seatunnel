## Deployment and run

> seatunnel v2 For Spark relies on the Java runtime environment and Spark. For detailed seatunnel installation steps, please refer to [installing seatunnel](./installation.md)

The following focuses on how different platforms operate:

## Run seatunnel locally in local mode

```bash
./bin/start-seatunnel-spark.sh \
--master local[4] \
--deploy-mode client \
--config ./config/application.conf
```

## Run seatunnel on Spark Standalone cluster

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

## Run seatunnel on Yarn cluster

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

## Run seatunnel on Mesos cluster

```bash
# cluster mode
./bin/start-seatunnel-spark.sh \
--master mesos://ip:7077 \
--deploy-mode cluster \
--config ./config/application.conf
```

For the meaning of the `master` and `deploy-mode` parameters of `start-seatunnel-spark.sh` , please refer to: [Command Instructions](./commands/start-seatunnel-spark.sh.md)

If you want to specify the resource size occupied by `seatunnel` when running, or other `Spark parameters` , you can specify it in the configuration file specified by `--config` :

```bash
env {
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  ...
}
...
```

For how to configure `seatunnel` , please refer to `seatunnel` [common configuration](./configuration)

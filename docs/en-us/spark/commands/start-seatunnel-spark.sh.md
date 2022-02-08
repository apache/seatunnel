# Command usage instructions

## seatunnel spark start command

```bash
bin/start-seatunnel-spark.sh
```

### usage instructions

```bash
bin/start-seatunnel-spark.sh \
-c config-path \
-m master \
-e deploy-mode \
-i city=beijing
```

- Use `-c` or `--config` to specify the path of the configuration file

- Use `-m` or `--master` to specify the cluster manager

- Use `-e` or `--deploy-mode` to specify the deployment mode

- Use `-i` or `--variable` to specify the variables in the configuration file, you can configure multiple

#### Use Cases

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

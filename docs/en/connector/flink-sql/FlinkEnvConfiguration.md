# Flink Env Configuration

## Description

We can configure the execution environment when we start the Flink process. Refer to the [Flink Configuration](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/deployment/config/) for more information.

## Usage

### 1. Flink Version 

Flink-1.13.6

### 2. seatunnel config

Prepare a seatunnel config file with the following content:

```bash
CONF parallelism.default = 3;
CONF state.backend = rocksdb;
-- set checkpoint path
CONF state.checkpoints.dir = file:///checkpoints-data;
-- set checkpoint interval
CONF execution.checkpointing.interval = 1min;

```

### 3. run job

```bash
./bin/start-seatunnel-sql.sh --config <path/to/your/config>
```




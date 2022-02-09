# Quick start

> Let's take an application that receives data through a `socket` , divides the data into multiple fields, and outputs the processing results as an example to quickly show how to use `seatunnel` .

## Step 1: Prepare Flink runtime environment

> If you are familiar with `Flink` or have prepared the `Flink` operating environment, you can ignore this step. `Flink` does not require any special configuration.

Please [download Flink](https://flink.apache.org/downloads.html) first, please choose Flink version >= 1.9.0. The download is complete to [install Flink](https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/deployment/resource-providers/standalone/overview/)

## Step 2: Download seatunnel

Enter the [seatunnel installation package](https://github.com/InterestingLab/seatunnel/releases) download page and download the latest version of `seatunnel-<version>.zip`

Or download the specified version directly (take 2.0.4 as an example):

```bash
wget https://github.com/InterestingLab/seatunnel/releases/download/v2.0.4/waterdrop-dist-2.0.4-2.11.8-release.zip -O seatunnel-2.0.4.zip
```

After downloading, unzip:

```bash
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

## Step 3: Configure seatunnel

- Edit `config/seatunnel-env.sh` , specify the necessary environment configuration such as `FLINK_HOME` (the directory after `Flink` downloaded and decompressed in Step 1)

- Edit `config/application.conf` , it determines the way and logic of data input, processing, and output after `seatunnel` is started.

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

## Step 4: Start the `netcat server` to send data

```bash
nc -l -p 9999
```

## Step 5: start `seatunnel`

```bash
cd seatunnel
./bin/start-seatunnel-flink.sh \
--config ./config/application.conf
```

## Step 6: Input at the `nc` terminal

```bash
xg#1995
```

It is printed in the TaskManager Stdout log of `flink WebUI`:

```bash
xg#1995,xg,1995
```

## Summary

If you want to know more `seatunnel` configuration examples, please refer to:

- Configuration example 1: [Streaming streaming computing](https://github.com/InterestingLab/seatunnel/blob/dev/config/flink.streaming.conf.template)

The above configuration is the default `[streaming configuration template]` , which can be run directly, the command is as follows:

```bash
cd seatunnel
./bin/start-seatunnel-flink.sh \
--config ./config/flink.streaming.conf.template
```

- Configuration example 2: [Batch offline batch processing](https://github.com/InterestingLab/seatunnel/blob/dev/config/flink.batch.conf.template)

The above configuration is the default `[offline batch configuration template]` , which can be run directly, the command is as follows:

```bash
cd seatunnel
./bin/start-seatunnel-flink.sh \
--config ./config/flink.batch.conf.template
```

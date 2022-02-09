# Quick start

> Let's take an application that receives data through a `socket` , divides the data into multiple fields, and outputs the processing results as an example to quickly show how to use `seatunnel`.

## Step 1: Prepare Spark runtime environment

> If you are familiar with Spark or have prepared the Spark operating environment, you can ignore this step. Spark does not require any special configuration.

Please [download Spark](https://spark.apache.org/downloads.html) first, please choose `Spark version >= 2.x.x` . After downloading and decompressing, you can submit the Spark `deploy-mode = local` mode task without any configuration. If you expect tasks to run on `Standalone clusters` or `Yarn clusters` or `Mesos clusters`, please refer to the [Spark deployment documentation](https://spark.apache.org/docs/latest/cluster-overview.html) on the Spark official website.

### Step 2: Download seatunnel

Enter the [seatunnel installation package download page](https://github.com/InterestingLab/seatunnel/releases) and download the latest version of `seatunnel-<version>.zip`

Or download the specified version directly (take `2.0.4` as an example):

```bash
wget https://github.com/InterestingLab/seatunnel/releases/download/v2.0.4/waterdrop-dist-2.0.4-2.11.8-release.zip -O seatunnel-2.0.4.zip
```

After downloading, unzip:

```bash
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

## Step 3: Configure seatunnel

- Edit `config/seatunnel-env.sh` , specify the necessary environment configuration such as `SPARK_HOME` (the directory after Spark downloaded and decompressed in Step 1)

- Create a new `config/application.conf` , which determines the method and logic of data input, processing, and output after `seatunnel` is started.

```bash
env {
  # seatunnel defined streaming batch duration in seconds
  spark.streaming.batchDuration = 5

  spark.app.name = "seatunnel"
  spark.ui.port = 13000
}

source {
  socketStream {}
}

transform {
  split {
    fields = ["msg", "name"]
    delimiter = ","
  }
}

sink {
  console {}
}
```

## Step 4: Start the `netcat server` to send data

```bash
nc -lk 9999
```

## Step 5: start seatunnel

```bash
cd seatunnel
./bin/start-seatunnel-spark.sh \
--master local[4] \
--deploy-mode client \
--config ./config/application.conf
```

## Step 6: Input at the `nc` terminal

```bash
Hello World, seatunnel
```

The `seatunnel` log prints out:

```bash
+----------------------+-----------+---------+
|raw_message           |msg        |name     |
+----------------------+-----------+---------+
|Hello World, seatunnel|Hello World|seatunnel|
+----------------------+-----------+---------+
```

## summary

`seatunnel` is simple and easy to use, and there are more abundant data processing functions waiting to be discovered. The data processing case shown in this article does not require any code, compilation, and packaging, and is simpler than the official [Quick Example](https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example).

If you want to know more `seatunnel configuration examples`, please refer to:

- Configuration example 2: [Batch offline batch processing](https://github.com/InterestingLab/seatunnel/blob/dev/config/spark.batch.conf.template)

The above configuration is the default [offline batch configuration template], which can be run directly, the command is as follows:

```bash
cd seatunnel
./bin/start-seatunnel-spark.sh \
--master 'local[2]' \
--deploy-mode client \
--config ./config/spark.batch.conf.template
```

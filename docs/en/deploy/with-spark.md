---

sidebar_position: 4
-------------------

# Quick Start With Spark

## Step 1: Deployment SeaTunnel And Connectors

Before starting, make sure you have downloaded and deployed SeaTunnel as described in [deployment](deployment.md)

## Step 2: Deployment And Config Spark

Please [download Spark](https://spark.apache.org/downloads.html) first(**required version >= 2.4.0**). For more information you could
see [Getting Started: standalone](https://spark.apache.org/docs/latest/spark-standalone.html#installing-spark-standalone-to-a-cluster)

**Configure SeaTunnel**: Change the setting in `config/seatunnel-env.sh`, it is base on the path your engine install at [deployment](deployment.md).
Change `SPARK_HOME` to the Spark deployment dir.

## Step 3: Add Job Config File to define a job

Edit `config/seatunnel.streaming.conf.template`, which determines the way and logic of data input, processing, and output after seatunnel is started.
The following is an example of the configuration file, which is the same as the example application mentioned above.

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FieldMapper {
    source_table_name = "fake"
    result_table_name = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    source_table_name = "fake1"
  }
}

```

More information about config please check [config concept](../../concept/config.md)

## Step 3: Run SeaTunnel Application

You could start the application by the following commands

spark 2.4.x

```bash
cd "apache-seatunnel-incubating-${version}"
./bin/start-seatunnel-spark-2-connector-v2.sh \
--master local[4] \
--deploy-mode client \
--config ./config/seatunnel.streaming.conf.template
```

spark3.x.x

```shell
cd "apache-seatunnel-incubating-${version}"
./bin/start-seatunnel-spark-3-connector-v2.sh \
--master local[4] \
--deploy-mode client \
--config ./config/seatunnel.streaming.conf.template
```

**See The Output**: When you run the command, you could see its output in your console. You can think this
is a sign that the command ran successfully or not.

The SeaTunnel console will prints some logs as below:

```shell
fields : name, age
types : STRING, INT
row=1 : elWaB, 1984352560
row=2 : uAtnp, 762961563
row=3 : TQEIB, 2042675010
row=4 : DcFjo, 593971283
row=5 : SenEb, 2099913608
row=6 : DHjkg, 1928005856
row=7 : eScCM, 526029657
row=8 : sgOeE, 600878991
row=9 : gwdvw, 1951126920
row=10 : nSiKE, 488708928
row=11 : xubpl, 1420202810
row=12 : rHZqb, 331185742
row=13 : rciGD, 1112878259
row=14 : qLhdI, 1457046294
row=15 : ZTkRx, 1240668386
row=16 : SGZCr, 94186144
```

## What's More

For now, you are already take a quick look about SeaTunnel with Spark, you could see [connector](/docs/category/connector-v2) to find all
source and sink SeaTunnel supported. Or see [SeaTunnel With Spark](../../other-engine/spark.md) if you want to know more about SeaTunnel Run With Spark.

SeaTunnel have an own engine named `Zeta` and `Zeta` is the default engine of SeaTunnel. You can follow [Quick Start](quick-start-seatunnel-engine.md) to configure and run a data synchronization job.

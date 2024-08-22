---
sidebar_position: 4
---

# Quick Start With Spark

## Step 1: Deployment SeaTunnel And Connectors

Before starting, make sure you have downloaded and deployed SeaTunnel as described in [Deployment](deployment.md)

## Step 2: Deploy And Config Spark

Please [Download Spark](https://spark.apache.org/downloads.html) first(**required version >= 2.4.0**). For more information you can
see [Getting Started: Standalone](https://spark.apache.org/docs/latest/spark-standalone.html#installing-spark-standalone-to-a-cluster)

**Configure SeaTunnel**: Change the setting in `${SEATUNNEL_HOME}/config/seatunnel-env.sh` and set `SPARK_HOME` to the Spark deployment dir.

## Step 3: Add Job Config File To Define A Job

Edit `config/seatunnel.streaming.conf.template`, which determines the way and logic of data input, processing, and output after seatunnel is started.
The following is an example of the configuration file, which is the same as the example application mentioned above.

```hocon
env {
  parallelism = 1
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

More information about config please check [Config Concept](../../concept/config.md)

## Step 4: Run SeaTunnel Application

You could start the application by the following commands:

Spark 2.4.x

```bash
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-spark-2-connector-v2.sh \
--master local[4] \
--deploy-mode client \
--config ./config/v2.streaming.conf.template
```

Spark3.x.x

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-spark-3-connector-v2.sh \
--master local[4] \
--deploy-mode client \
--config ./config/v2.streaming.conf.template
```

**See The Output**: When you run the command, you can see its output in your console. This
is a sign to determine whether the command ran successfully or not.

The SeaTunnel console will print some logs as below:

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

- Start write your own config file now, choose the [connector](../../connector-v2/source) you want to use, and configure the parameters according to the connector's documentation.
- See [SeaTunnel With Spark](../../other-engine/spark.md) if you want to know more about SeaTunnel With Spark.
- SeaTunnel have a builtin engine named `Zeta`, and it's the default engine of SeaTunnel. You can follow [Quick Start](quick-start-seatunnel-engine.md) to configure and run a data synchronization job.


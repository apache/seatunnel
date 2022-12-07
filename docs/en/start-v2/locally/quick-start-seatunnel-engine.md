---
sidebar_position: 2
---

# Quick Start With SeaTunnel Engine

## Step 1: Deployment SeaTunnel And Connectors

Before starting, make sure you have downloaded and deployed SeaTunnel as described in [deployment](deployment.md)

## Step 2: Add Job Config File to define a job

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

}

sink {
  Console {}
}

```

More information about config please check [config concept](../../concept/config.md)

## Step 3: Run SeaTunnel Application

You could start the application by the following commands

```shell
cd "apache-seatunnel-incubating-${version}"
./bin/seatunnel.sh --config ./config/seatunnel.streaming.conf.template -e local

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

For now, you are already take a quick look about SeaTunnel, you could see [connector](/docs/category/connector-v2) to find all
source and sink SeaTunnel supported. Or see [SeaTunnel Engine](../../seatunnel-engine/about.md) if you want to know more about SeaTunnel Engine.

SeaTunnel also supports running jobs in Spark/Flink. You can see [Quick Start With Spark](quick-start-spark.md) or [Quick Start With Flink](quick-start-flink.md).

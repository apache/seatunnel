---
sidebar_position: 2
---

# Quick Start With SeaTunnel Engine

## Step 1: Deployment SeaTunnel And Connectors

Before starting, make sure you have downloaded and deployed SeaTunnel as described in [deployment](deployment.md)

## Step 2: Add Job Config File to define a job

Edit `config/v2.batch.conf.template`, which determines the way and logic of data input, processing, and output after seatunnel is started.
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
2022-12-19 11:01:45,417 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=1:  SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CpiOd, 8520946
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=2 : eQqTs, 1256802974
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=3 : UsRgO, 2053193072
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=4 : jDQJj, 1993016602
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=5 : rqdKp, 1392682764
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=6 : wCoWN, 986999925
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=7 : qomTU, 72775247
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=8 : jcqXR, 1074529204
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=9 : AkWIO, 1961723427
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=10 : hBoib, 929089763
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=11 : GSvzm, 827085798
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=12 : NNAYI, 94307133
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=13 : EexFl, 1823689599
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=14 : CBXUb, 869582787
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=15 : Wbxtm, 1469371353
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=16 : mIJDt, 995616438
```

## What's More

For now, you are already take a quick look about SeaTunnel, you could see [connector](../../connector-v2/source/FakeSource.md) to find all
source and sink SeaTunnel supported. Or see [SeaTunnel Engine](../../seatunnel-engine/about.md) if you want to know more about SeaTunnel Engine.

SeaTunnel also supports running jobs in Spark/Flink. You can see [Quick Start With Spark](quick-start-spark.md) or [Quick Start With Flink](quick-start-flink.md).

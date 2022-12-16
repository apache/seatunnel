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
2022-12-16 23:46:16,713 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - fields: name, age
2022-12-16 23:46:16,714 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - types: STRING, INT
2022-12-16 23:46:17,753 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=1 : XRMxN, 1011284614
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=2 : pWrpO, 843127905
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=3 : REBSo, 833504132
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=4 : WsFRN, 26587682
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=5 : vTuoq, 414594867
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=6 : CskBS, 1855664254
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=7 : HHYfL, 627284161
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=8 : twFct, 964724571
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=9 : JGLqE, 1919562939
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=10 : WDZLH, 1649520200
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=11 : lcOVg, 1666394529
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=12 : TnyqU, 2132613196
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=13 : tSHoE, 179597991
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=14 : RbFsX, 1726762920
2022-12-16 23:46:17,755 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=15 : wsMAq, 46289844
2022-12-16 23:46:17,755 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=16 : GXOQx, 1675062543
```

## What's More

For now, you are already take a quick look about SeaTunnel, you could see [connector](../../connector-v2/source/FakeSource.md) to find all
source and sink SeaTunnel supported. Or see [SeaTunnel Engine](../../seatunnel-engine/about.md) if you want to know more about SeaTunnel Engine.

SeaTunnel also supports running jobs in Spark/Flink. You can see [Quick Start With Spark](quick-start-spark.md) or [Quick Start With Flink](quick-start-flink.md).

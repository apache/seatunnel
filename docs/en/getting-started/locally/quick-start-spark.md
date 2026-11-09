---
sidebar_position: 4
---

# Quick Start With Spark

## Before You Begin

This page is for teams that already want SeaTunnel to run on Spark. If you are evaluating SeaTunnel for the first time and do not specifically need Spark, start with [Quick Start With SeaTunnel Engine](./quick-start-seatunnel-engine.md) first.

Read these pages first if needed:

- [Engine Overview](../../engines/overview.md)
- [SeaTunnel With Spark](../../engines/spark.md)
- [Job Configuration Guide](../job-configuration-guide.md)

## Step 1: Deployment SeaTunnel And Connectors

Before starting, make sure you have downloaded and deployed SeaTunnel as described in [Deployment](deployment.md)

## Step 2: Deploy And Config Spark

Please [Download Spark](https://spark.apache.org/downloads.html) first(**required version >= 2.4.0**). For more information you can
see [Getting Started: Standalone](https://spark.apache.org/docs/latest/spark-standalone.html#installing-spark-standalone-to-a-cluster)

**Configure SeaTunnel**: Change the setting in `${SEATUNNEL_HOME}/config/seatunnel-env.sh` and set `SPARK_HOME` to the Spark deployment dir.

## Step 3: Add Job Config File To Define A Job

For Spark 3.5, edit `config/v2.batch.config.template` with `job.mode = "BATCH"`.
The bounded Spark 3.5.8 micro-batch test covers the row encoder and streaming sink
adapter. Streaming through the public starter and checkpoint restore remain
unvalidated, so the streaming template is not a verified Spark 3.5 example. Existing
Spark 2.4/3.3 deployments can continue
using `config/v2.streaming.conf.template` as shown below.
The following is an example of the configuration file, which is the same as the example application mentioned above.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
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
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}

```

More information about config please check [Config Concept](../../introduction/concepts/config.md)

If you need more details about dataset wiring or transform parameters, continue with:

- [Job Configuration Guide](../job-configuration-guide.md)
- [Transform Common Options](../../transforms/common-options/common-options.md)

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

Spark 3.3.x

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-spark-3-connector-v2.sh \
--master local[4] \
--deploy-mode client \
--config ./config/v2.streaming.conf.template
```

Spark 3.4.x

For an existing Spark 3.4 deployment, retain `seatunnel-spark-3-starter.jar` and the
`start-seatunnel-spark-3-connector-v2` launcher shown above. This change adds no
separate Spark 3.4 starter and no new Spark 3.4 compatibility validation.

Spark 3.5.x

Start with a batch job. The bounded micro-batch sink-adapter test does not validate
streaming through the public starter or checkpoint restore; the streaming template
is not part of this quick start for Spark 3.5.

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-spark-3.5-connector-v2.sh \
--master local[4] \
--deploy-mode client \
--config ./config/v2.batch.config.template
```

**See The Output**: When you run the command, you can see its output in your console. This
is a sign to determine whether the command ran successfully or not.

The SeaTunnel console will print some logs as below:

The log prefix (time, thread, logger layout) and the `tableId` value depend on the engine's logging configuration and the job, so they are shown as `...` below. `FieldMapper` outputs the fields in the order of `field_mapper` and renames `name` to `new_name`, so the output row type is `age<INT>, new_name<STRING>`.

```shell
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: age<INT>, new_name<STRING>
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=1:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1984352560, elWaB
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=2:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 762961563, uAtnp
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=3:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 2042675010, TQEIB
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=4:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 593971283, DcFjo
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=5:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 2099913608, SenEb
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=6:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1928005856, DHjkg
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=7:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 526029657, eScCM
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=8:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 600878991, sgOeE
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=9:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1951126920, gwdvw
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=10:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 488708928, nSiKE
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=11:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1420202810, xubpl
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=12:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 331185742, rHZqb
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=13:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1112878259, rciGD
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=14:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1457046294, qLhdI
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=15:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1240668386, ZTkRx
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=16:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 94186144, SGZCr
```

## What's More

- Start write your own config file now, choose the [connector](../../connectors/source) you want to use, and configure the parameters according to the connector's documentation.
- See [SeaTunnel With Spark](../../engines/spark.md) if you want to know more about SeaTunnel With Spark.
- See [Spark Translation Layer](../../architecture/api-design/spark-translation-layer.md) if you want to understand how SeaTunnel APIs are adapted to Spark.
- SeaTunnel has a builtin engine named `Zeta`, and it is the default engine of SeaTunnel. You can follow [Quick Start With SeaTunnel Engine](quick-start-seatunnel-engine.md) if you want the shortest local validation path.

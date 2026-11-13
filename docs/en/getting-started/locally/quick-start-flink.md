---
sidebar_position: 3
---

# Quick Start With Flink

## Before You Begin

This page is for teams that already want SeaTunnel to run on Flink. If you are evaluating SeaTunnel for the first time and do not specifically need Flink, start with [Quick Start With SeaTunnel Engine](./quick-start-seatunnel-engine.md) first.

Read these pages first if needed:

- [Engine Overview](../../engines/overview.md)
- [SeaTunnel With Flink](../../engines/flink.md)
- [Job Configuration Guide](../job-configuration-guide.md)

## Step 1: Deploy SeaTunnel And Connectors

Before starting, make sure you have downloaded and deployed SeaTunnel as described in [Deployment](deployment.md)

## Step 2: Deployment And Config Flink

Please [Download Flink](https://flink.apache.org/downloads.html) first(**required version >= 1.12.0**). For more information you can see [Getting Started: Standalone](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/)

**Configure SeaTunnel**: Change the setting in `${SEATUNNEL_HOME}/config/seatunnel-env.sh` and set `FLINK_HOME` to the Flink deployment dir.

## Step 3: Add Job Config File To Define A Job

Edit `config/v2.streaming.conf.template`, which determines the way and logic of data input, processing, and output after seatunnel is started.
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

You can start the application by the following commands:

Flink version between `1.12.x` and `1.14.x`

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-13-connector-v2.sh --config ./config/v2.streaming.conf.template
```

Flink version between `1.15.x` and `1.18.x`

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-15-connector-v2.sh --config ./config/v2.streaming.conf.template
```

Flink version `1.20.x`

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-20-connector-v2.sh --config ./config/v2.streaming.conf.template
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
- See [SeaTunnel With Flink](../../engines/flink.md) if you want to know more about SeaTunnel With Flink.
- See [Flink Translation Layer](../../architecture/api-design/flink-translation-layer.md) if you want to understand how SeaTunnel APIs are adapted to Flink.
- SeaTunnel has a builtin engine named `Zeta`, and it is the default engine of SeaTunnel. You can follow [Quick Start With SeaTunnel Engine](quick-start-seatunnel-engine.md) if you want the shortest local validation path.

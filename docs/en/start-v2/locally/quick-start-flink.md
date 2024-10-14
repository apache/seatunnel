---
sidebar_position: 3
---

# Quick Start With Flink

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
- See [SeaTunnel With Flink](../../other-engine/flink.md) if you want to know more about SeaTunnel With Flink.
- SeaTunnel have a builtin engine named `Zeta`, and it's the default engine of SeaTunnel. You can follow [Quick Start](quick-start-seatunnel-engine.md) to configure and run a data synchronization job.


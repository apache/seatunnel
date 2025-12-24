---
sidebar_position: 2
---

# Quick Start With SeaTunnel Engine

## Step 1: Deploy SeaTunnel And Connectors

Before starting, make sure you have downloaded and deployed SeaTunnel as described in [Deployment](deployment.md)

## Step 2: Add Job Config File To Define A Job

Edit `config/v2.batch.config.template`, which determines the way and logic of data input, processing, and output after seatunnel is started.
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

More information can be found in [Config Concept](../../concept/config.md)

## Step 3: Run SeaTunnel Application

You could start the application by the following commands:

:::tip

Starting from version 2.3.1, the parameter -e in seatunnel.sh is deprecated, use -m instead.

:::

```shell
cd "apache-seatunnel-${version}"
./bin/seatunnel.sh --config ./config/v2.batch.config.template -m local

```

**See The Output**: When you run the command, you can see its output in your console. This
is a sign to determine whether the command ran successfully or not.

The SeaTunnel console will print some logs as below:

```shell
2022-12-19 11:01:45,417 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=1:  SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CpiOd, 8520946
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=2: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: eQqTs, 1256802974
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=3: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: UsRgO, 2053193072
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=4: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jDQJj, 1993016602
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=5: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: rqdKp, 1392682764
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=6: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: wCoWN, 986999925
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=7: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: qomTU, 72775247
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=8: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jcqXR, 1074529204
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=9: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: AkWIO, 1961723427
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=10: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: hBoib, 929089763
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=11: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: GSvzm, 827085798
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=12: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: NNAYI, 94307133
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=13: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: EexFl, 1823689599
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=14: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CBXUb, 869582787
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=15: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: Wbxtm, 1469371353
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=16: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: mIJDt, 995616438
```

## Extended Example: Batch Mode from MySQL to Doris

### Step 1: Download the Connector

First, you need to add the connector name to the `${SEATUNNEL_HOME}/config/plugin_config` file. Then, execute the command to install the connector (of course, you can also manually download the connector from the [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) and move it to the `connectors/` directory). Finally, make sure that the `connector-jdbc` and `connector-doris` connectors are in the `${SEATUNNEL_HOME}/connectors/` directory.

```bash
# Configure the connector name.
--seatunnel-connectors--
connector-jdbc
connector-doris
--end--
```

```bash
# Install the connector.
sh bin/install-plugin.sh
```

### Step 2: Place the MySQL Driver

You need to download the [JDBC driver JAR package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) and place it in the `${SEATUNNEL_HOME}/lib/` directory.

### Step 3: Add Job Configuration File to Define the Job

```bash
cd seatunnel/job/

vim st.conf

env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://localhost:3306/test"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "user"
        password = "pwd"
        table_path = "test.table_name"
        query = "select  * from test.table_name"
    }
}

sink {
   Doris {
          fenodes = "doris_ip:8030"
          username = "user"
          password = "pwd"
          database = "test_db"
          table = "table_name"
          sink.enable-2pc = "true"
          sink.label-prefix = "test-cdc"
          doris.config = {
            format = "json"
            read_json_by_line="true"
          }
      }
}
```

For more information about the configuration, please refer to [Basic Concepts of Configuration](../../concept/config.md).

### Step 4: Run the SeaTunnel Application

You can start the application using the following command:

```shell
cd seatunnel/
./bin/seatunnel.sh --config ./job/st.conf -m local

```

**Check the Output**: When you run the command, you can see its output in the console. You can consider this as an indicator of whether the command has succeeded or failed.

The SeaTunnel console will print some log information like the following:

```shell
***********************************************
           Job Statistic Information
***********************************************
Start Time                : 2024-08-13 10:21:49
End Time                  : 2024-08-13 10:21:53
Total Time(s)             :                   4
Total Read Count          :                1000
Total Write Count         :                1000
Total Failed Count        :                   0
***********************************************
```

:::tip

If you want to optimize your job, refer to the connector documentation for [Source-MySQL](../../connector-v2/source/Mysql.md) and [Sink-Doris](../../connector-v2/sink/Doris.md).

:::


## What's More

- Start write your own config file now, choose the [connector](../../connector-v2/source) you want to use, and configure the parameters according to the connector's documentation.
- See [SeaTunnel Engine(Zeta)](../../seatunnel-engine/about.md) if you want to know more about SeaTunnel Engine. Here you will learn how to deploy SeaTunnel Engine and how to use it in cluster mode.


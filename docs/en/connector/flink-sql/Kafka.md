# Flink SQL Kafka Connector

> Kafka connector based by flink sql

## Description

With kafka connector, we can read data from kafka and write data to kafka using Flink SQL. Refer to the [Kafka connector](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/kafka/) for more details.


## Usage
Let us have a brief example to show how to use the connector from end to end.

### 1. kafka prepare
Please refer to the [Kafka QuickStart](https://kafka.apache.org/quickstart) to prepare kafka environment and produce data like following:

```bash
$ bin/kafka-console-producer.sh --topic <topic-name> --bootstrap-server localhost:9092
```

After executing the command, we will come to the interactive mode. Print the following message to send data to kafka.
```bash
{"id":1,"name":"abc"}
>{"id":2,"name":"def"}
>{"id":3,"name":"dfs"}
>{"id":4,"name":"eret"}
>{"id":5,"name":"yui"}
```

### 2. prepare seatunnel configuration
Here is a simple example of seatunnel configuration.
```sql
SET table.dml-sync = true;

CREATE TABLE events (
    id INT,
    name STRING
) WITH (
    'connector' = 'kafka',
    'topic'='<topic-name>',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE print_table (
    id INT,
    name STRING
) WITH (
    'connector' = 'print',
    'sink.parallelism' = '1'
);

INSERT INTO print_table SELECT * FROM events;
```

### 3. start flink local cluster
```bash
$ ${FLINK_HOME}/bin/start-cluster.sh
```

### 4. start Flink SQL job
Execute the following command in seatunnel home path to start the Flink SQL job.
```bash
$ bin/start-seatunnel-sql.sh -c config/kafka.sql.conf
```

### 5. verify result
After the job submitted, we can see the data printing by connector 'print' in taskmanager's log .
```text
+I[1, abc]
+I[2, def]
+I[3, dfs]
+I[4, eret]
+I[5, yui]
```

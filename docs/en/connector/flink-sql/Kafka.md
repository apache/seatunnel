# Flink SQL Kafka Connector

## Description

With kafka connector, we can read data from kafka and write data to kafka using Flink SQL. Refer to the [Kafka connector](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/kafka/) for more details.


## Usage
Let us have a brief example to show how to use the connector from end to end.

### 1. kafka prepare
If you have the kafka environment, you can directly go to step 3.

We can download kafka distribution from [Apache Kafka](https://kafka.apache.org/downloads.html).

After decompressing the kafka distribution, we can get the following files.
```bash
$ ls
LICENSE   NOTICE    bin       config    libs      licenses  logs      site-docs
```

Before starting kafka server, we should start the zookeeper server by executing the following command.
```bash
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```

Open another terminal and start kafka server.
```bash
$ bin/kafka-server-start.sh config/server.properties
```

If the shell process not exit, it means kafka server start successfully.

### 2. prepare data
Create kafka topic by executing the following command.
```bash
$ bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
```

Start the producer process.
```bash
$ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
```

After executing the command, we will come to the interactive mode. Print the following message to send data to kafka.
```bash
{"id":1,"name":"abc"}
>{"id":2,"name":"def"}
>{"id":3,"name":"dfs"}
>{"id":4,"name":"eret"}
>{"id":5,"name":"yui"}
```

Start the consumer process.
```bash
$ bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

When the consumer process start, we can see the data produced above.
```bash
{"id":1,"name":"abc"}
{"id":2,"name":"def"}
{"id":3,"name":"dfs"}
{"id":4,"name":"eret"}
{"id":5,"name":"yui"}
```

### 3. prepare seatunnel configuration
Here is a simple example of seatunnel configuration.
```sql
SET table.dml-sync = true;

CREATE TABLE events (
    id INT,
    name STRING
) WITH (
    'connector' = 'kafka',
    'topic'='quickstart-events',
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

### 4. start flink local cluster
```bash
$ ${FLINK_HOME}/bin/start-cluster.sh
```

### 5. start Flink SQL job
Execute the following command in seatunnel home path to start the Flink SQL job.
```bash
$ bin/start-seatunnel-sql.sh -c config/kafka.sql.conf
```

### 6. verify result
After the job submitted, we can see the data printing by connector 'print' in taskmanager's log .
```text
+I[1, abc]
+I[2, def]
+I[3, dfs]
+I[4, eret]
+I[5, yui]
```

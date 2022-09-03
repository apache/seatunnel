# Intro to connector V2 features

## Source Connector Features

### exactly-once

If each piece of data in the data source will only be sent downstream by the source once, we think this source connector supports exactly once.

In SeaTunnel, we can save the read **Split** and its **offset**(The position of the read data in split at that time,
such as line number, byte size, offset, etc) as **StateSnapshot** when checkpoint. If the task restarted, we will get the last **StateSnapshot**
and then locate the **Split** and **offset** read last time and continue to send data downstream.

For example `File`, `Kafka`.

### schema projection

If the source connector supports selective reading of certain columns or supports the data format read through `schema` params, we think it supports schema projection.

For example `JDBCSource` can use sql define read columns, `KafkaSource` can use `schema` params to define the read schema.

### batch

Batch Job Mode, The data read is bounded and the job will stop when all data read complete.

### stream

Streaming Job Mode, The data read is unbounded and the job never stop.

### parallelism

Parallelism Source Connector support config `parallelism`, every parallelism will create a task to read the data.

## Sink Connector Features

### exactly-once

When any piece of data flows into a distributed system, if the system processes any piece of data accurately only once in the whole processing process and the processing results are correct, it is considered that the system meets the exact once consistency.

For sink connector, the sink connector supports exactly-once if any piece of data only write into target once. There are generally two ways to achieve this:

* The target database supports key deduplication. For example `MySQL`, `Kudu`.
* The target support **XA Transaction**(This transaction can be used across sessions. Even if the program that created the transaction has ended, the newly started program only needs to know the ID of the last transaction to resubmit or roll back the transaction). Then we can use **Two-phase Commit** to ensure **exactly-once**. For example `File`, `MySQL`.

### schema projection

If a sink connector supports the fields and their types written in the configuration, we think it supports schema projection.
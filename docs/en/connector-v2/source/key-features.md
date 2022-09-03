# Sink Connector Key Features

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
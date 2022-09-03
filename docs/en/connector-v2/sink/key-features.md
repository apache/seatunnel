# Sink Connector Key Features

### exactly-once

When any piece of data flows into a distributed system, if the system processes any piece of data accurately only once in the whole processing process and the processing results are correct, it is considered that the system meets the exact once consistency.

For sink connector, the sink connector supports exactly-once if any piece of data only write into target once. There are generally two ways to achieve this:

* The target database supports key deduplication. For example `MySQL`, `Kudu`.
* The target support **XA Transaction**(This transaction can be used across sessions. Even if the program that created the transaction has ended, the newly started program only needs to know the ID of the last transaction to resubmit or roll back the transaction). Then we can use **Two-phase Commit** to ensure **exactly-once**. For example `File`, `MySQL`.

### schema projection

If a sink connector supports the fields and their types written in the configuration, we think it supports schema projection.
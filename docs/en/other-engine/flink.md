# Seatunnel runs on Flink

Flink is a powerful high-performance distributed stream processing engine,More information about it you can,You can search for `Apacke Flink`

### Set Flink configuration information in the job

Begin with `flink.`

Example:
I set a precise Checkpoint for this job

```
env {
  execution.parallelism = 1  
  flink.execution.checkpointing.mode="EXACTLY_ONCE"
}
```

### How to set up a simple Flink job

This is a simple job that runs on Flink Randomly generated data is printed to the console

```
env {
  execution.parallelism = 1
  flink.execution.checkpointing.mode="EXACTLY_ONCE"
}

source {
  FakeSource {
    row.num = 5
    int.template = [2]
    result_table_name = "mongodb_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(33, 18)"
        c_timestamp = timestamp
        c_row = {
          c_map = "map<string, string>"
          c_array = "array<int>"
          c_string = string
          c_boolean = boolean
          c_int = int
          c_bigint = bigint
          c_double = double
          c_bytes = bytes
          c_date = date
          c_decimal = "decimal(33, 18)"
          c_timestamp = timestamp
        }
      }
    }
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

Sink{
   Console{}   
}
```

### How to run a job in a project

After you pull the code to the local, go to the `seatunnel-examples/seatunnel-flink-connector-v2-example` module find `org.apache.seatunnel.example.flink.v2.SeaTunnelApiExample` To complete the operation of the job

### If you want to hand in an assignment

[Quick Start With Flink](quick-start-flink.md)

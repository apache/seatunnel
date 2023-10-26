# Seatunnel runs on Flink

Flink is a powerful high-performance distributed stream processing engine,More information about it you can,You can search for `Apacke Flink`

### Set Flink configuration information in the job

Begin with `flink.`

Example:
I set a precise Checkpoint for this job

```
env {
  execution.parallelism = 1  
  flink.execution.checkpointing.unaligned.enabled=true
}
```

Enumeration types are not currently supported, you need to specify them in the Flink conf file ,Only these types of Settings are supported for the time being:<br/>
Integer/Boolean/String/Duration

### How to set up a simple Flink job

This is a simple job that runs on Flink Randomly generated data is printed to the console

```
env {
  execution.parallelism = 1
  flink.execution.checkpointing.interval=5000
}

source {
  FakeSource {
    row.num = 16
    result_table_name = "fake_table"
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

sink{
   Console{}   
}
```

### How to run a job in a project

After you pull the code to the local, go to the `seatunnel-examples/seatunnel-flink-connector-v2-example` module find `org.apache.seatunnel.example.flink.v2.SeaTunnelApiExample` To complete the operation of the job

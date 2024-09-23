# SeaTunnel Runs On Spark

Spark is a powerful high-performance distributed calculate processing engine. More information about it you can search for `Apache Spark`

### Set Spark Configuration Information In The Job

Example:
I set a conf for this job

```
env {
  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory= "2g"
  spark.executor.instances = "2"
  spark.yarn.priority = "100'
  hive.exec.dynamic.partition.mode = "nonstrict"
  spark.dynamicAllocation.enabled="false"
}
```

### Command Line Example

#### Spark on Yarn Cluster

```
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode cluster --config config/example.conf
```

#### Spark on Yarn Cluster

```
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

### How To Set Up A Simple Spark Job

This is a simple job that runs on Spark. Randomly generated data is printed to the console

```
env {
  # common parameter
  parallelism = 1

  # spark special parameter
  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory= "2g"
  spark.executor.instances = "1"
  spark.yarn.priority = "100"
  hive.exec.dynamic.partition.mode = "nonstrict"
  spark.dynamicAllocation.enabled="false"
}

source {
  FakeSource {
  schema = {
    fields {
      c_map = "map<string, array<int>>"
      c_array = "array<int>"
      c_string = string
      c_boolean = boolean
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
      c_decimal = "decimal(30, 8)"
      c_null = "null"
      c_bytes = bytes
      c_date = date
      c_timestamp = timestamp
      c_row = {
        c_map = "map<string, map<string, string>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
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

### How To Run A Job In A Project

After you pull the code to the local, go to the `seatunnel-examples/seatunnel-spark-connector-v2-example` module and find `org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample` to complete the operation of the job.

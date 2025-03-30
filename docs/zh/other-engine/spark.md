# SeaTunnel 通过 Spark 引擎运行

Spark 是一个强大的高性能分布式计算处理引擎。有关它的更多信息，您可以搜索"Apache Spark"


### 如何在作业中设置 Spark 配置信息

例：
我为这个任务设置了一些 spark 配置项

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

### 命令行示例

#### Spark on Yarn集群

```
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode cluster --config config/example.conf
```

#### Spark on Yarn集群

```
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

### 如何设置简单的 Spark 作业

这是通过 Spark 运行的一个简单作业。会将随机生成的数据输出到控制台

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
}

sink{
   Console{}   
}
```

### 如何在项目中运行作业

将代码拉取到本地后，进入 seatunnel-examples/seatunnel-spark-connector-v2-example 模块，找到 org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample 来完成作业的运行。
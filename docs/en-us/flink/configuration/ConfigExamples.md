# Full configuration file example [Flink]

An example is as follows:

> In the configuration, the behavior comment beginning with `#`.

```bash
######
###### This config file is a demonstration of streaming processing in seatunnel config
######

env {
    # You can set flink configuration here
    execution.parallelism = 1
    #execution.checkpoint.interval = 10000
    #execution.checkpoint.data-uri = "hdfs://localhost:9000/checkpoint"
}

source {
    # This is a example source plugin **only for test and demonstrate the feature source plugin**
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }

    # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
    # please go to https://interestinglab.github.io/seatunnel-docs/#/zh-cn/configuration/base
}

transform {
    sql {
      sql = "select name,age from fake"
    }

    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://interestinglab.github.io/seatunnel-docs/#/zh-cn/configuration/base
}

sink {
    ConsoleSink {}

    # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
    # please go to https://interestinglab.github.io/seatunnel-docs/#/zh-cn/configuration/base
}
```

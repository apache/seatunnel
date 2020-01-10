## 完整配置文件案例 [Flink]


一个示例如下：

>配置中, 以#开头的行为注释。

```
######
###### This config file is a demonstration of streaming processing in waterdrop config
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

  # If you would like to get more information about how to configure waterdrop and see full list of source plugins,
  # please go to https://interestinglab.github.io/waterdrop/#/zh-cn/configuration/base
}

transform {
    sql {
      sql = "select name,age from fake"
    }

  # If you would like to get more information about how to configure waterdrop and see full list of transform plugins,
  # please go to https://interestinglab.github.io/waterdrop/#/zh-cn/configuration/base
}

sink {
  ConsoleSink {}


  # If you would like to get more information about how to configure waterdrop and see full list of sink plugins,
  # please go to https://interestinglab.github.io/waterdrop/#/zh-cn/configuration/base
}
```
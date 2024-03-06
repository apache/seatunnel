---

sidebar_position: 2
-------------------

# 配置文件简介

In SeaTunnel, the most important thing is the Config file, through which users can customize their own data
synchronization requirements to maximize the potential of SeaTunnel. So next, I will introduce you how to
configure the Config file.

在SeaTunnel中，最重要的事情就是配置文件，尽管用户可以自定义他们自己的数据同步需求以发挥SeaTunnel最大的潜力。那么接下来，
我将会向你介绍如何设置配置文件。

The main format of the Config file is `hocon`, for more details of this format type you can refer to [HOCON-GUIDE](https://github.com/lightbend/config/blob/main/HOCON.md),
BTW, we also support the `json` format, but you should know that the name of the config file should end with `.json`

配置文件的主要格式是 `hocon`, 有关该格式类型的更多信息你可以参考[HOCON-GUIDE](https://github.com/lightbend/config/blob/main/HOCON.md),
顺便提一下，我们也支持 `json`格式，但你应该知道配置文件的名称应该是以 `.json`结尾。

## 例子

在你阅读之前，你可以在发布包中的config目录[这里](https://github.com/apache/seatunnel/tree/dev/config)找到配置文件的例子。

## 配置文件结构

配置文件类似下面。

### hocon

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
        card = "int"
      }
    }
  }
}

transform {
  Filter {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [name, card]
  }
}

sink {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    table = "seatunnel_console"
    fields = ["name", "card"]
    username = "default"
    password = ""
    source_table_name = "fake1"
  }
}
```

### json

```json

{
  "env": {
    "job.mode": "batch"
  },
  "source": [
    {
      "plugin_name": "FakeSource",
      "result_table_name": "fake",
      "row.num": 100,
      "schema": {
        "fields": {
          "name": "string",
          "age": "int",
          "card": "int"
        }
      }
    }
  ],
  "transform": [
    {
      "plugin_name": "Filter",
      "source_table_name": "fake",
      "result_table_name": "fake1",
      "fields": ["name", "card"]
    }
  ],
  "sink": [
    {
      "plugin_name": "Clickhouse",
      "host": "clickhouse:8123",
      "database": "default",
      "table": "seatunnel_console",
      "fields": ["name", "card"],
      "username": "default",
      "password": "",
      "source_table_name": "fake1"
    }
  ]
}

```

正如你看到的，配置文件包括几个部分：env, source, transform, sink。不同的模块有不同的功能。
当你了解了这些模块后，你就会懂得SeaTunnel如何工作。

### env

用于添加引擎可选的参数，不管是什么引擎（Spark 或者 Flink），对应的可选参数应该在这里填写。

注意，我们按照引擎分离了参数，对于公共参数，我们可以像以前一样配置。对于Flink和Spark引擎，其参数的具体配置规则可以参考[JobEnvConfig](./JobEnvConfig.md)。

<!-- TODO add supported env parameters -->

### source

source用于定义SeaTunnel在哪儿检索数据，并将检索的数据用于下一步。
可以同时定义多个source。目前支持的source请看[Source of SeaTunnel](../../en/connector-v2/source)。每种source都有自己特定的参数用来
定义如何检索数据，SeaTunnel也抽象了每种source所使用的参数，例如 `result_table_name` 参数，用于指定当前source生成的数据的名称，
方便后续其他模块使用。

### transform

当我们有了数据源之后，我们可能需要对数据进行进一步的处理，所以我们就有了transform模块。当然，这里使用了“可能”这个词，
这意味着我们也可以直接将transform视为不存在，直接从source到sink。像下面这样。

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
        card = "int"
      }
    }
  }
}

sink {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    table = "seatunnel_console"
    fields = ["name", "age", "card"]
    username = "default"
    password = ""
    source_table_name = "fake1"
  }
}
```

与source类似, transform也有属于每个模块的特定参数。目前支持的source请看。目前支持的transform请看 [Transform V2 of SeaTunnel](../../en/transform-v2)

<!-- TODO missing source links --->

### sink

我们使用SeaTunnel的作用是将数据从一个地方同步到其它地方，所以定义数据如何写入，写入到哪里是至关重要的。通过SeaTunnel提供的
sink模块，你可以快速高效地完成这个操作。Sink和source非常相似，区别在于读取和写入。所以去看看我们[支持的sink](../../en/connector-v2/sink)吧。

### 其它

你会疑惑当定义了多个source和多个sink时，每个sink读取哪些数据，每个transform读取哪些数据？我们使用`result_table_name` 和
`source_table_name` 两个键配置。每个source模块都会配置一个`result_table_name`来指示数据源生成的数据源名称，其它transform和sink
模块可以使用`source_table_name` 引用相应的数据源名称，表示要读取数据进行处理。然后transform，作为一个中间的处理模块，可以同时使用
`result_table_name` 和 `source_table_name` 配置。但你会发现在上面的配置例子中，不是每个模块都配置了这些参数，因为在SeaTunnel中，
有一个默认的约定，如果这两个参数没有配置，则使用上一个节点的最后一个模块生成的数据。当只有一个source时这是非常方便的。

## 此外

如果你想了解更多关于格式配置的详细信息，请查看 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md)。

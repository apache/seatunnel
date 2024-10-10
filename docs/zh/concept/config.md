# 配置文件简介

在SeaTunnel中，最重要的事情就是配置文件，尽管用户可以自定义他们自己的数据同步需求以发挥SeaTunnel最大的潜力。那么接下来我将会向你介绍如何设置配置文件。

配置文件的主要格式是 `hocon`, 有关该格式类型的更多信息你可以参考[HOCON-GUIDE](https://github.com/lightbend/config/blob/main/HOCON.md),
顺便提一下，我们也支持 `json`格式，但你应该知道配置文件的名称应该是以 `.json`结尾。

我们同时提供了以 `SQL` 格式，详细可以参考[SQL配置文件](sql-config.md)。

## 例子

在你阅读之前，你可以在发布包中的config目录[这里](https://github.com/apache/seatunnel/tree/dev/config)找到配置文件的例子。

## 配置文件结构

配置文件类似下面这个例子：

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

正如你看到的，配置文件包括几个部分：env, source, transform, sink。不同的模块具有不同的功能。
当你了解了这些模块后，你就会懂得SeaTunnel到底是如何工作的。

### env

用于添加引擎可选的参数，不管是什么引擎（Zeta、Spark 或者 Flink），对应的可选参数应该在这里填写。

注意，我们按照引擎分离了参数，对于公共参数我们可以像以前一样配置。对于Flink和Spark引擎，其参数的具体配置规则可以参考[JobEnvConfig](./JobEnvConfig.md)。

<!-- TODO add supported env parameters -->

### source

source用于定义SeaTunnel在哪儿检索数据，并将检索的数据用于下一步。
可以同时定义多个source。目前支持的source请看[Source of SeaTunnel](../../en/connector-v2/source)。每种source都有自己特定的参数用来
定义如何检索数据，SeaTunnel也抽象了每种source所使用的参数，例如 `result_table_name` 参数，用于指定当前source生成的数据的名称，
方便后续其他模块使用。

### transform

当我们有了数据源之后，我们可能需要对数据进行进一步的处理，所以我们就有了transform模块。当然，这里使用了“可能”这个词，
这意味着我们也可以直接将transform视为不存在，直接从source到sink，像下面这样：

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
sink模块，你可以快速高效地完成这个操作。Sink和source非常相似，区别在于读取和写入。所以去看看我们[Sink of SeaTunnel](../../en/connector-v2/sink)吧。

### 其它

你会疑惑当定义了多个source和多个sink时，每个sink读取哪些数据，每个transform读取哪些数据？我们使用`result_table_name` 和
`source_table_name` 两个配置。每个source模块都会配置一个`result_table_name`来指示数据源生成的数据源名称，其它transform和sink
模块可以使用`source_table_name` 引用相应的数据源名称，表示要读取数据进行处理。然后transform，作为一个中间的处理模块，可以同时使用
`result_table_name` 和 `source_table_name` 配置。但你会发现在上面的配置例子中，不是每个模块都配置了这些参数，因为在SeaTunnel中，
有一个默认的约定，如果这两个参数没有配置，则使用上一个节点的最后一个模块生成的数据。当只有一个source时这是非常方便的。

## 多行文本支持

`hocon`支持多行字符串，这样就可以包含较长的文本段落，而不必担心换行符或特殊格式。这可以通过将文本括在三层引号 **`"""`** 中来实现。例如:

```
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
sql = """ select * from "table" """
```

## Json格式支持

在编写配置文件之前，请确保配置文件的名称应以 `.json` 结尾。

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

## 配置变量替换

在配置文件中,我们可以定义一些变量并在运行时替换它们。但是注意仅支持 hocon 格式的文件。

变量使用方法：
 - `${varName}`，如果变量未传值，则抛出异常。
 - `${varName:default}`，如果变量未传值，则使用默认值。如果设置默认值则变量需要写在双引号中。
 - `${varName:}`，如果变量未传值，则使用空字符串。

如果您不通过`-i`设置变量值，也可以通过设置系统的环境变量传值，变量替换支持通过环境变量获取变量值。
例如，您可以在shell脚本中设置环境变量如下：
```shell
export varName="value with space"
```
然后您可以在配置文件中使用变量。

如果您在配置文件中设置了没有默认值的变量，但在执行过程中未传递该变量，则会保留该变量值，系统不会抛出异常。但请您需要确保其他流程能够正确解析该变量值。例如，ElasticSearch的索引需要支持`${xxx}`这样的格式来动态指定索引。若其他流程不支持，程序可能无法正常运行。

具体样例：
```hocon
env {
  job.mode = "BATCH"
  job.name = ${jobName}
  parallelism = 2
}

source {
  FakeSource {
    result_table_name = "${resName:fake_test}_table"
    row.num = "${rowNum:50}"
    string.template = ${strTemplate}
    int.template = [20, 21]
    schema = {
      fields {
        name = "${nameType:string}"
        age = ${ageType}
      }
    }
  }
}

transform {
    sql {
      source_table_name = "${resName:fake_test}_table"
      result_table_name = "sql"
      query = "select * from ${resName:fake_test}_table where name = '${nameVal}' "
    }

}

sink {
  Console {
     source_table_name = "sql"
     username = ${username}
     password = ${password}
  }
}
```

在上述配置中,我们定义了一些变量,如 ${rowNum}、${resName}。
我们可以使用以下 shell 命令替换这些参数:

```shell
./bin/seatunnel.sh -c <this_config_file> 
-i jobName='this_is_a_job_name' 
-i strTemplate=['abc','d~f','hi'] 
-i ageType=int
-i nameVal=abc 
-i username=seatunnel=2.3.1 
-i password='$a^b%c.d~e0*9(' 
-m local
```

其中 `resName`，`rowNum`，`nameType` 我们未设置，他将获取默认值


然后最终提交的配置是:

```hocon
env {
  job.mode = "BATCH"
  job.name = "this_is_a_job_name"
  parallelism = 2
}

source {
  FakeSource {
    result_table_name = "fake_test_table"
    row.num = 50
    string.template = ['abc','d~f','hi']
    int.template = [20, 21]
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
    sql {
      source_table_name = "fake_test_table"
      result_table_name = "sql"
      query = "select * from fake_test_table where name = 'abc' "
    }

}

sink {
  Console {
     source_table_name = "sql"
     username = "seatunnel=2.3.1"
     password = "$a^b%c.d~e0*9("
    }
}

```

一些注意事项:

- 如果值包含特殊字符，如`(`，请使用`'`引号将其括起来。
- 如果替换变量包含`"`或`'`(如`"resName"`和`"nameVal"`)，需要添加`"`。
- 值不能包含空格`' '`。例如, `-i jobName='this is a job name'`将被替换为`job.name = "this"`。 你可以使用环境变量传递带有空格的值。 
- 如果要使用动态参数,可以使用以下格式: `-i date=$(date +"%Y%m%d")`。
- 不能使用指定系统保留字符，它将不会被`-i`替换，如:`${database_name}`、`${schema_name}`、`${table_name}`、`${schema_full_name}`、`${table_full_name}`、`${primary_key}`、`${unique_key}`、`${field_names}`。具体可参考[Sink参数占位符](sink-options-placeholders.md)
## 此外

如果你想了解更多关于格式配置的详细信息，请查看 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md)。


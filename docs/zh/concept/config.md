# 配置文件简介

配置文件是SeaTunnel中最核心的组成部分。用户可以通过配置文件自定义数据同步需求，充分发挥SeaTunnel的能力。本文将详细介绍配置文件的设置方法。

配置文件主要使用`hocon`格式，关于该格式的详细信息可参考[HOCON-GUIDE](https://github.com/lightbend/config/blob/main/HOCON.md)。
同时也支持`json`格式，但需要注意配置文件必须以`.json`作为后缀。

此外，SeaTunnel还支持`SQL`格式的配置文件，详细信息请参考[SQL配置文件](sql-config.md)。

## 示例

在SeaTunnel发布包的config目录中提供了配置文件示例，可以在[这里](https://github.com/apache/seatunnel/tree/dev/config)查看。

## 配置文件结构

以下是一个典型的配置文件示例：

:::caution 警告
配置项 `result_table_name`/`source_table_name` 已废弃，请尽快迁移到新的配置项 `plugin_output`/`plugin_input`。
:::

### hocon

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
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
    plugin_input = "fake"
    plugin_output = "fake1"
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
    plugin_input = "fake1"
  }
}
```

正如你看到的，配置文件包括几个部分：env, source, transform, sink。不同的模块具有不同的功能。
当你了解了这些模块后，你就会懂得SeaTunnel到底是如何工作的。

### env

用于配置引擎相关的可选参数。无论是使用Zeta、Spark还是Flink引擎，相应的配置参数都应该在此处设置。

需要注意的是，参数配置已按引擎类型进行了分离。通用参数可以按原有方式配置，而Flink和Spark引擎的特定参数配置规则请参考[JobEnvConfig](./JobEnvConfig.md)。

<!-- TODO add supported env parameters -->

### source

source模块定义了数据的来源位置以及数据获取方式。系统支持同时配置多个source。当前支持的source类型请参考[Source of SeaTunnel](../connector-v2/source)。
每种source都有其特定的参数配置，用于定义数据获取方式。SeaTunnel为所有source抽象了一些通用参数，例如`plugin_output`参数用于指定当前source输出的数据集名称，
便于后续模块引用。

### transform

获取数据后，可能需要进行数据处理，这就是transform模块的作用。transform模块是可选的，也可以直接从source到sink，示例如下：

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
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
    plugin_input = "fake"
  }
}
```

与source类似，每个transform模块都有其特定的参数配置。目前支持的transform类型请参考[Transform V2 of SeaTunnel](../transform-v2)。

### sink

sink模块定义了数据的写入位置和方式，是实现数据同步的关键环节。通过SeaTunnel提供的sink模块，可以高效地完成数据写入操作。
sink的配置方式与source类似，区别在于数据流向的不同。支持的sink类型请参考[Sink of SeaTunnel](../connector-v2/sink)。

### 数据流转说明

当配置了多个source和sink时，如何确定数据的流转关系？这就需要用到`plugin_output`和`plugin_input`两个配置项：
- source模块通过`plugin_output`指定输出的数据集名称
- transform和sink模块通过`plugin_input`指定要处理的数据集来源
- transform作为中间处理模块，可以同时配置`plugin_input`和`plugin_output`

值得注意的是，这两个参数不是必须配置的。在SeaTunnel中有一个默认约定：如果未配置这两个参数，系统会自动使用上一个节点最后一个模块的输出作为输入。这在只有单一数据流的场景下特别方便。

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
      "plugin_output": "fake",
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
      "plugin_input": "fake",
      "plugin_output": "fake1",
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
      "plugin_input": "fake1"
    }
  ]
}

```

## 配置变量替换

配置文件支持变量替换功能（仅支持hocon格式）。变量的使用方式如下：
- `${varName}` - 若变量未赋值则抛出异常
- `${varName:default}` - 若变量未赋值则使用默认值（变量需要用双引号包围）
- `${varName:}` - 若变量未赋值则使用空字符串

变量值的传入有两种方式：
1. 通过`-i`参数直接传入
2. 通过系统环境变量设置，例如：
```shell
export varName="value with space"
```

特别说明：如果配置文件中使用了没有默认值的变量，且执行时未传入该变量值，系统会保留原变量形式而不会抛出异常。这种情况下需要确保后续处理流程能够正确解析该变量值，
例如ElasticSearch的索引支持`${xxx}`这样的动态索引格式。如果其他流程不支持该格式，可能会导致程序执行异常。

具体样例：
```hocon
env {
  job.mode = "BATCH"
  job.name = ${jobName}
  parallelism = 2
}

source {
  FakeSource {
    plugin_output = "${resName:fake_test}_table"
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
      plugin_input = "${resName:fake_test}_table"
      plugin_output = "sql"
      query = "select * from ${resName:fake_test}_table where name = '${nameVal}' "
    }

}

sink {
  Console {
     plugin_input = "sql"
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
    plugin_output = "fake_test_table"
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
      plugin_input = "fake_test_table"
      plugin_output = "sql"
      query = "select * from dual where name = 'abc' "
    }

}

sink {
  Console {
     plugin_input = "sql"
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
- 值不能包含空格`' '`。例如, `-i jobName='this is a job name'`将被替换为`job.name = "this"`。 你可以使用环境变量传递带有空格的值。 
- 如果要使用动态参数,可以使用以下格式: `-i date=$(date +"%Y%m%d")`。
- 不能使用指定系统保留字符，它将不会被`-i`替换，如:`${database_name}`、`${schema_name}`、`${table_name}`、`${schema_full_name}`、`${table_full_name}`、`${primary_key}`、`${unique_key}`、`${field_names}`。具体可参考[Sink参数占位符](sink-options-placeholders.md)
## 此外

如果你想了解更多关于格式配置的详细信息，请查看 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md)。


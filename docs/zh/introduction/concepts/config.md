# 配置文件简介

如果您正在编写第一个真正可用的 SeaTunnel 作业，这一页最重要的目标，就是帮您先理解几乎所有配置里都会出现的四个部分：`env`、`source`、`transform`、`sink`。

SeaTunnel 支持 `hocon`、`json` 和 `SQL` 三种配置格式。其中 **HOCON** 是快速开始和生产示例中最常见的格式。SQL 格式请参考 [SQL 配置文件](../configuration/sql-config.md)。

如果您还没有跑通过第一个任务，建议先阅读 [快速入门总览](../../getting-started/overview.md) 和 [SeaTunnel 引擎快速开始](../../getting-started/locally/quick-start-seatunnel-engine.md)，再回到这一页。

## 例子

继续往下看之前，您可以先在发布包的 `config` 目录，或者 [这里](https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e/connector-jdbc-e2e-part-1/src/test/resources) 查看示例配置。

## 配置文件结构

配置文件类似下面这个例子：

:::caution 警告

旧的配置名称 `result_table_name`/`source_table_name` 已经过时，请尽快迁移到新名称 `plugin_output`/`plugin_input`。

:::

### HOCON 示例

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

大多数 SeaTunnel 作业都会遵循 `env`、`source`、`transform`、`sink` 这四段结构。只要先理解这四段，后面读快速开始、连接器示例和真实任务配置都会容易很多。

### `env`：作业与引擎参数

`env` 用来放作业级和引擎级参数，比如 `job.mode`、`parallelism`、checkpoint 相关配置，以及引擎特有参数。

公共参数可以直接配置；引擎专属参数则按前缀区分。Flink 和 Spark 的具体写法请参考 [JobEnvConfig](../configuration/JobEnvConfig.md)。

<!-- TODO add supported env parameters -->

### `source`：数据读取入口

`source` 用来定义 SeaTunnel 从哪里读取数据。一个作业中可以同时声明多个 source。每个连接器都有自己的参数，同时也有一些通用的链路字段，例如 `plugin_output`，它用于给当前 source 产出的数据集命名，方便后续模块引用。

完整列表请查看 [数据来源连接器](../../connectors/source-overview.md)。

### `transform`：中间处理步骤

`transform` 是可选的。当您需要做字段映射、过滤、类型转换、SQL 处理，或者其它中间加工时，就使用这一层；如果不需要，也可以直接从 source 到 sink，例如下面这样：

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

和 source 一样，每个 transform 也有自己的专属参数。完整列表请查看 [数据转换目录](../../transforms)。

### `sink`：数据写入目标

`sink` 用来定义处理后的数据写到哪里去。它和 source 很相似，但更关注写入行为、目标表结构、提交方式以及投递保证。

完整列表请查看 [数据写入连接器](../../connectors/sink-overview.md)。

### `plugin_output` 和 `plugin_input` 是怎么工作的

当一个作业里同时存在多个 source、transform 或 sink 时，SeaTunnel 需要知道“哪一份数据流向下一步的哪个模块”。这就是 `plugin_output` 和 `plugin_input` 的作用。

- `plugin_output` 给当前 source 或 transform 产出的数据集命名
- `plugin_input` 告诉下游 transform 或 sink 应该消费哪一个上游数据集

如果只是单一 source 的简单链路，很多时候可以省略它们，因为 SeaTunnel 会按默认约定自动把上一个模块的输出继续往下传递。

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

## JSON 格式支持

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

在配置文件中，我们可以定义一些变量并在运行时替换它们。但请注意，目前仅支持 HOCON 格式的文件。

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
- 不能使用指定系统保留字符，它将不会被`-i`替换，如:`${database_name}`、`${schema_name}`、`${table_name}`、`${schema_full_name}`、`${table_full_name}`、`${primary_key}`、`${unique_key}`、`${field_names}`、`${partition_keys}`。具体可参考[Sink参数占位符](../configuration/sink-options-placeholders.md)
## 此外

- 现在就可以开始写自己的配置文件，选择要使用的 [连接器](../../connectors/source-overview.md)，再按对应文档填写参数。
- 如果您需要按引擎配置参数，请继续阅读 [JobEnvConfig](../configuration/JobEnvConfig.md)。
- 如果您想了解更完整的语法细节，请查看 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md)。

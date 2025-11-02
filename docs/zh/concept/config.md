# 配置文件介绍

在 SeaTunnel 中，配置文件至关重要。用户可以通过它自定义数据同步需求，从而最大限度地发挥 SeaTunnel 的潜力。接下来，本文将为您介绍如何配置该文件。

配置文件的主要格式是 `hocon`，更多详情请参阅 [HOCON 指南](https://github.com/lightbend/config/blob/main/HOCON.md)。此外，我们也支持 `json` 格式，但请注意，配置文件的名称应以 `.json` 结尾。

我们同样支持 `SQL` 格式，更多详情请参阅 [SQL 配置](sql-config.md)。

## 示例

在继续阅读之前，您可以在二进制包的 `config` 目录下找到配置文件示例，具体路径[在此](https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e/connector-jdbc-e2e-part-1/src/test/resources)。

## 配置文件结构

配置文件结构大致如下：

:::caution 警告

旧的配置项名称 `source_table_name`/`result_table_name` 已被弃用，请尽快迁移到新的名称 `plugin_input`/`plugin_output`。

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

如您所见，配置文件包含几个部分：env、source、transform、sink。不同的模块具有不同的功能。在理解这些模块后，您将明白 SeaTunnel 的工作原理。

### env

用于添加一些引擎的可选参数。无论是哪种引擎（Zeta、Spark 或 Flink），相应的可选参数都应在此处填写。

请注意，我们已经按引擎对参数进行了分离，对于通用参数，可以像以前一样配置。对于 Flink 和 Spark 引擎，其参数的具体配置规则可以参考 [JobEnvConfig](./JobEnvConfig.md)。

<!-- TODO add supported env parameters -->

### source

Source 用于定义 SeaTunnel 需要从何处获取数据，并将获取的数据用于下一步处理。可以同时定义多个 Source。支持的 Source 列表可以在 [SeaTunnel 的 Source](../connector-v2/source) 中找到。每个 Source 都有其特定的参数来定义如何获取数据。同时，SeaTunnel 也提取了每个 Source 都会使用的通用参数，例如 `plugin_output` 参数，它用于指定当前 Source 插件产生的数据的名称，以方便后续其他模块使用。

### transform

当我们有了数据源后，可能需要对数据进行进一步处理，因此我们引入了 transform 模块。当然，这里用了“可能”这个词，意味着我们也可以直接将 transform 模块视作不存在，直接将数据从 source 输入到 sink，如下所示。

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

与 Source 类似，Transform 也有属于各个模块的特定参数。支持的 Transform 列表可以在 [SeaTunnel 的 Transform V2](../transform-v2) 中找到。

### sink

我们使用 SeaTunnel 的目的是将数据从一个地方同步到另一个地方，因此定义数据写入的方式和位置至关重要。通过 SeaTunnel 提供的 sink 模块，您可以快速高效地完成此操作。Sink 和 Source 非常相似，区别在于读和写。请查阅[支持的 Sinks](../connector-v2/sink)。

### 其他信息

您会发现，当定义了多个 Source 和多个 Sink 时，每个 Sink 读取的是哪些数据？每个 Transform 读取的又是哪些数据？为此，我们引入了两个关键配置：`plugin_output` 和 `plugin_input`。每个 Source 模块都会配置一个 `plugin_output` 来表明该数据源生成的数据名称，而其他的 Transform 和 Sink 模块则可以通过 `plugin_input` 来引用相应的数据源名称，表示“我要读取这个数据进行处理”。而 Transform 作为一个中间处理模块，可以同时使用 `plugin_output` 和 `plugin_input` 配置。但您会发现，在上面的配置示例中，并非每个模块都配置了这两个参数。这是因为在 SeaTunnel 中存在一个默认约定：如果未配置这两个参数，那么将默认使用前一个节点的最后一个模块产生的数据。这在只有一个 Source 的情况下会方便得多。

## 多行字符串支持

在 `hocon` 格式中，支持多行字符串。这使您可以包含大段文本，而无需担心换行符或特殊格式。实现方式是将文本包裹在三引号 **`"""`** 之内。例如：

```
var = """
Apache SeaTunnel 是一款
下一代高性能、
分布式、海量数据集成工具。
"""
sql = """ select * from "table" """
```

## Json 格式支持

在编写配置文件之前，请确保文件名以 `.json` 结尾。

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

在配置文件中，我们可以定义变量并在运行时进行替换。但请注意，仅支持 HOCON 格式的文件。

### 变量用法：
- `${varName}`: 如果未提供该变量，将会抛出异常。
- `${varName:default}`: 如果未提供该变量，将使用默认值。若设置默认值，应将其包含在双引号内。
- `${varName:}`: 如果未提供该变量，将使用空字符串。

如果您不通过 `-i` 参数设置变量值，也可以通过设置系统环境变量来传递值。变量替换支持通过环境变量获取变量值。
例如，您可以在 shell 脚本中设置环境变量如下：
```shell
export varName="value with space"
```
然后您就可以在配置文件中使用该变量。

如果您在配置文件中设置了一个没有默认值的变量，但在执行时没有传递它，该变量的值将被保留，系统不会抛出异常。但请确保其他进程能够正确解析该变量值。例如，ElasticSearch 的索引需要支持 `${xxx}` 这样的格式来动态指定索引。如果其他进程不支持，程序可能无法正常运行。

### 示例：
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
}```

在上述配置中，我们定义了 `${rowNum}`、`${resName}` 等几个变量。我们可以使用以下 shell 命令来替换这些参数：

```shell
./bin/seatunnel.sh -c <this_config_file> \
-i jobName='this_is_a_job_name' \
-i strTemplate=['abc','d~f','hi'] \
-i ageType=int \
-i nameVal=abc \
-i username=seatunnel=2.3.1 \
-i password='$a^b%c.d~e0*9(' \
-m local
```

在这种情况下，`resName`、`rowNum` 和 `nameType` 未被设置，因此它们将取用各自的默认值。

最终提交的配置将是：

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

### 重要注意事项：
- 如果值包含特殊字符（如 `(`），请用单引号 (`'`) 括起来。
- 如果替换的变量包含双引号或单引号（例如 `"resName"` 或 `"nameVal"`），您需要将它们与值一起提供。
- 值不能包含空格 (`' '`)。例如，`-i jobName='this is a job name'` 将被替换为 `job.name = "this"`。您可以使用环境变量来传递带空格的值。
- 对于动态参数，您可以使用以下格式：`-i date=$(date +"%Y%m%d")`。
- 不能使用指定的系统保留字符；它们不会被 `-i` 替换，例如：`${database_name}`、`${schema_name}`、`${table_name}`、`${schema_full_name}`、`${table_full_name}`、`${primary_key}`、`${unique_key}`、`${field_names}`。详情请参阅 [Sink 参数占位符](sink-options-placeholders.md)。

## 更多内容

- 现在就开始编写您自己的配置文件，选择您想使用的[连接器](../connector-v2/source)，并根据连接器的文档配置参数。
- 如果您想了解格式配置的详细信息，请参阅 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md)。

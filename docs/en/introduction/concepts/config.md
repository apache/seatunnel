# Intro To Config File

If you are writing your first real SeaTunnel job, this page is the fastest way to understand the four blocks that appear in almost every config: `env`, `source`, `transform`, and `sink`.

SeaTunnel supports `hocon`, `json`, and `SQL` config formats. HOCON is the most common format in quick starts and production examples. For SQL format, see [SQL configuration](../configuration/sql-config.md).

If you want the shortest first-run path before reading this page, start with [Getting Started Overview](../../getting-started/overview.md) and [Quick Start With SeaTunnel Engine](../../getting-started/locally/quick-start-seatunnel-engine.md).

## Example

Before you read on, you can find example configs [here](https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e/connector-jdbc-e2e-part-1/src/test/resources) and in the binary package's `config` directory.

## Config File Structure

The config file is similar to the below one:

:::caution warn

The old configuration name `source_table_name`/`result_table_name` is deprecated, please migrate to the new name `plugin_input`/`plugin_output` as soon as possible.

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

Most SeaTunnel jobs follow this structure: `env`, `source`, `transform`, and `sink`. Once you understand these four sections, it becomes much easier to read quick starts and connector examples.

### env

Use `env` for job-level and engine-level settings such as `job.mode`, `parallelism`, checkpoint options, and engine-specific parameters.

Common parameters are shared across engines. Engine-specific parameters are separated by prefix. For Flink and Spark, see [JobEnvConfig](../configuration/JobEnvConfig.md).

<!-- TODO add supported env parameters -->

### source

`source` defines where SeaTunnel reads data from. You can declare multiple sources in one job. Each connector has its own parameters, plus common wiring fields such as `plugin_output`, which names the dataset produced by that source.

See the full list in [Source Connectors](../../connectors/source).

### transform

`transform` is optional. Use it when you need field mapping, filtering, type conversion, SQL processing, or other intermediate shaping between source and sink. If you do not need that layer, a job can go directly from source to sink, like this:

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

Like source connectors, each transform has its own parameters. See [Transforms](../../transforms).

### sink

`sink` defines where the processed data is written. Sink connectors are similar to source connectors, but they focus on write behavior, destination schema, commit mode, and delivery guarantees.

See [Supported Sinks](../../connectors/sink).

### How `plugin_output` And `plugin_input` Work

When a job contains multiple sources, transforms, or sinks, SeaTunnel needs a way to describe which dataset flows into which next step. That wiring is done by `plugin_output` and `plugin_input`.

- `plugin_output` names the dataset produced by the current source or transform
- `plugin_input` tells a transform or sink which upstream dataset to consume

In simple one-source jobs, you can often omit them because SeaTunnel uses a default convention and passes the previous module's output forward automatically.

## Multi-line Support

In `hocon`, multiline strings are supported, which allows you to include extended passages of text without worrying about newline characters or special formatting. This is achieved by enclosing the text within triple quotes **`"""`** . For example:

```
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
sql = """ select * from "table" """
```

## Json Format Support

Before writing the config file, please make sure that the name of the config file should end with `.json`.

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

## Config Variable Substitution

In a config file, we can define variables and replace them at runtime. However, note that only HOCON format files are supported.

### Usage of Variables:
- `${varName}`: If the variable is not provided, an exception will be thrown.
- `${varName:default}`: If the variable is not provided, the default value will be used. If you set a default value, it should be enclosed in double quotes.
- `${varName:}`: If the variable is not provided, an empty string will be used.

If you do not set the variable value through `-i`, you can also pass the value by setting the system environment variables. Variable substitution supports obtaining variable values through environment variables.
For example, you can set the environment variable in the shell script as follows:
```shell
export varName="value with space"
```
Then you can use the variable in the config file.

If you set a variable without a default value in the configuration file but do not pass it during execution, the value of the variable will be retained and the system will not throw an exception. But please ensure that other processes can correctly parse the variable value. For example, ElasticSearch's index needs to support a format like '${xxx}' to dynamically specify the index. If other processes are not supported, the program may not run properly.


### Example:
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

In the configuration above, we have defined several variables like `${rowNum}`, `${resName}`. We can replace these parameters using the following shell command:

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

In this case, `resName`, `rowNum`, and `nameType` are not set, so they will take their default values.

The final submitted configuration would be:

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

### Important Notes:
- If a value contains special characters like `(`, enclose it in single quotes (`'`).
- If the substitution variable contains double or single quotes (e.g., `"resName"` or `"nameVal"`), you need to include them with the value.
- The value cannot contain spaces (`' '`). For example, `-i jobName='this is a job name'` will be replaced with `job.name = "this"`. You can use environment variables to pass values with spaces.
- For dynamic parameters, you can use the following format: `-i date=$(date +"%Y%m%d")`.
- Cannot use specified system reserved characters; they will not be replaced by `-i`, such as: `${database_name}`, `${schema_name}`, `${table_name}`, `${schema_full_name}`, `${table_full_name}`, `${primary_key}`, `${unique_key}`, `${field_names}`, `${partition_keys}`. For details, please refer to [Sink Parameter Placeholders](../configuration/sink-options-placeholders.md).

## What's More

- Start writing your own config file now, choose the [connector](../../connectors/source) you want to use, and configure it according to the connector documentation.
- See [JobEnvConfig](../configuration/JobEnvConfig.md) when you need engine-specific settings.
- See [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) if you want the full syntax details.

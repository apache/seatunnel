---
sidebar_position: 3
---

# Source Common Options

> Source connector 的常用参数

|        名称         |   类型   | 必填 | 默认值 |                                                                                                                                    描述                                                                                                                                     |
|-------------------|--------|----|-----|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| result_table_name | String | 否  | -   | 当未指定 `result_table_name` 时，此插件处理的数据将不会被注册为可由其他插件直接访问的数据集 `(dataStream/dataset)`，或称为临时表 `(table)`。<br/>当指定了 `result_table_name` 时，此插件处理的数据将被注册为可由其他插件直接访问的数据集 `(dataStream/dataset)`，或称为临时表 `(table)`。此处注册的数据集 `(dataStream/dataset)` 可通过指定 `source_table_name` 直接被其他插件访问。 |
| parallelism       | Int    | 否  | -   | 当未指定 `parallelism` 时，默认使用环境中的 `parallelism`。<br/>当指定了 `parallelism` 时，将覆盖环境中的 `parallelism` 设置。                                                                                                                                                                           |

# 重要提示

在作业配置中使用 `result_table_name` 时，必须设置 `source_table_name` 参数。

## 任务示例

### 简单示例

> 注册一个流或批处理数据源，并在注册时返回表名 `fake_table`

```bash
source {
    FakeSourceStream {
        result_table_name = "fake_table"
    }
}
```

### 复杂示例

> 这是将Fake数据源转换并写入到两个不同的目标中

```bash
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        c_timestamp = "timestamp"
        c_date = "date"
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_decimal = "decimal(30, 8)"
        c_row = {
          c_row = {
            c_int = int
          }
        }
      }
    }
  }
}

transform {
  Sql {
    source_table_name = "fake"
    result_table_name = "fake1"
    # 查询表名必须与字段 'source_table_name' 相同
    query = "select id, regexp_replace(name, '.+', 'b') as name, age+1 as age, pi() as pi, c_timestamp, c_date, c_map, c_array, c_decimal, c_row from fake"
  }
  # SQL 转换支持基本函数和条件操作
  # 但不支持复杂的 SQL 操作，包括：多源表/行 JOIN 和聚合操作等
}

sink {
  Console {
    source_table_name = "fake1"
  }
   Console {
    source_table_name = "fake"
  }
}
```


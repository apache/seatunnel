# SQL

> SQL 转换插件

## 描述

使用 SQL 来转换给定的输入行。

SQL 转换使用内存中的 SQL 引擎，我们可以通过 SQL 函数和 SQL 引擎的能力来实现转换任务。

## 属性

|        名称         |   类型   | 是否必须 | 默认值 |
|-------------------|--------|------|-----|
| source_table_name | string | yes  | -   |
| result_table_name | string | yes  | -   |
| query             | string | yes  | -   |

### source_table_name [string]

源表名称，查询 SQL 表名称必须与此字段匹配。

### query [string]

查询 SQL，它是一个简单的 SQL，支持基本的函数和条件过滤操作。但是，复杂的 SQL 尚不支持，包括：多源表/行连接和聚合操作等。

## 示例

源端数据读取的表格如下：

| id |   name   | age |
|----|----------|-----|
| 1  | Joy Ding | 20  |
| 2  | May Ding | 21  |
| 3  | Kin Dom  | 24  |
| 4  | Joy Dom  | 22  |

我们使用 SQL 查询来转换源数据，类似这样：

```
transform {
  Sql {
    source_table_name = "fake"
    result_table_name = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from fake where id>0"
  }
}
```

那么结果表 `fake1` 中的数据将会更新为：

| id |   name    | age |
|----|-----------|-----|
| 1  | Joy Ding_ | 21  |
| 2  | May Ding_ | 22  |
| 3  | Kin Dom_  | 25  |
| 4  | Joy Dom_  | 23  |

## 作业配置示例

```
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
      }
    }
  }
}

transform {
  Sql {
    source_table_name = "fake"
    result_table_name = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from fake where id>0"
  }
}

sink {
  Console {
    source_table_name = "fake1"
  }
}
```

## 更新日志

### 新版本

- 添加SQL转换连接器


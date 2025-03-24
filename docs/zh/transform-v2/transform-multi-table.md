---
sidebar_position: 2
---

# Transform的多表转换

SeaTunnel transform支持多表转换，在上游插件输出多个表的时候特别有用，能够在一个transform中完成所有的转换操作。目前SeaTunnel很多Connectors支持多表输出，比如`JDBCSource`、`MySQL-CDC`
等。所有的Transform都可以通过如下配置实现多表转换。

:::tip

多表Transform没有对Transform能力的限制，任何Transform的配置都可以在多表Transform中使用。多表Transform的作用针对数据流中的多个表进行单独的处理，并将多个表的Transform配置合并到一个Transform中，方便用户管理。

:::

## 属性

| Name                       | Type   | Required | Default | Description                                                                                      |
|----------------------------|--------|----------|---------|--------------------------------------------------------------------------------------------------|
| table_match_regex          | String | No       | .*      | 表名的正则表达式，通过正则表达式来匹配需要进行转换的表，默认匹配所有的表。注意这个表名是上游的真正表名，不是`plugin_output`。                           |
| table_transform            | List   | No       | -       | 可以通过table_transform列表来指定部分表的规则，当在table_transform中配置某个表的转换规则后，外层针对当前表的规则不会生效，以table_transform中的为准 |
| table_transform.table_path | String | No       | -       | 当在table_transform中配置某个表的转换规则后，需要使用table_path字段指定表名，表名需要包含`databaseName[.schemaName].tableName`。  |

## 匹配逻辑

假设我们从上游读取了5张表，分别为`test.abc`，`test.abcd`，`test.xyz`，`test.xyzxyz`，`test.www`。他们的表结构一致，都有`id`、`name`、`age`三个字段。

| id | name | age |

现在我们想通过Copy transform将这5张表的数据进行复制，具体需求是，`test.abc`，`test.abcd`表需要将`name`复制为`name1`，
`test.xyz`表需要复制为`name2`，`test.xyzxyz`表需要复制为`name3`，`test.www`数据结构不变。那么我们可以通过如下配置来实现：

```hocon
transform {
  Copy {
    plugin_input = "fake"  // 可选的读取数据集名
    plugin_output = "fake1" // 可选的输出数据集名

    table_match_regex = "test.a.*" // 1. 通过正则表达式匹配需要进行转换的表，test.a.*表示匹配test.abc和test.abcd
    src_field = "name" // 源字段
    dest_field = "name1" // 目标字段
    table_transform = [{
      table_path = "test.xyz" // 2. 指定表名进行转换
      src_field = "name"  // 源字段
      dest_field = "name2" // 目标字段
    }, {
      table_path = "test.xyzxyz"
      src_field = "name"
      dest_field = "name3"
    }]
  }
}
```

### 解释

1. 通过第一层的正则表达式，和对应的Copy transform options配置，我们可以匹配到`test.abc`和`test.abcd`表，将`name`字段复制为`name1`。
2. 通过`table_transform`配置，我们可以指定`test.xyz`表，将`name`字段复制为`name2`。

这样我们就可以通过一个transform完成对多个表的转换操作。

对于每个表来说，配置的优先级是：`table_transform` > `table_match_regex`。如果所有的规则都没有匹配到，那么该表将不会进行任何转换操作。

针对每个表来说，他们的Transform配置是：

- **test.abc**和**test.abcd**

```hocon
transform {
  Copy {
    src_field = "name"
    dest_field = "name1"
  }
}
```

输出表结构：

| id | name | age | name1 |

- **test.xyz**

```hocon
transform {
  Copy {
    src_field = "name"
    dest_field = "name2"
  }
}
```

输出表结构：

| id | name | age | name2 |

- **test.xyzxyz**

```hocon
transform {
  Copy {
    src_field = "name"
    dest_field = "name3"
  }
}
```

输出表结构：

| id | name | age | name3 |

- **test.www**

```hocon
transform {
  // 无需转换
}
```

输出表结构：

| id | name | age |

我们使用了Copy Transform作为了示例，实际上所有的Transform都支持多表转换，只需要在对应的Transform中配置即可。


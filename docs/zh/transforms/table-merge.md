# 多表合并

> TableMerge：将分库分表或多张来源表合并为统一输出表

## 描述

TableMerge 转换插件用于将分库分表或多张来源表合并为统一输出表。

## 参数

| 参数名             | 类型   | 是否必填 | 默认值        | 说明                                                                  |
|:------------------:|--------|----------|---------------|-----------------------------------------------------------------------|
| database           | string | 否       |               | 合并后新的 database 名称                                                |
| schema             | string | 否       |               | 合并后新的 schema 名称                                                  |
| table              | string | 是       |               | 合并后新的 table 名称                                                   |
| table_match_regex  | string | 否       | `.*`          | 匹配表路径的正则表达式，transform 级别的规则只对匹配到的表生效。                |
| table_transform    | array  | 否       | -             | 按表配置的转换规则，参见[多表转换](transform-multi-table.md)。                |
| rule_match_mode    | string | 否       | `FIRST_MATCH` | 当多条 `table_transform` 规则的 `table_path` 重复时的匹配方式。              |

既没有命中 `table_match_regex`、也没有配置对应 `table_transform` 规则的表将原样透传。在下面的示例中，`source.shop` 不匹配 `source.user_.*`，因此保持原表名不变，而 `source.user_1` 和 `source.user_2` 会被合并为 `user_db.user_all`。

## 示例

### 合并分库分表为一个表

```hocon
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
    MySQL-CDC {
        plugin_output = "customers_mysql_cdc"
        
        username = "root"
        password = "123456"
        table-names = ["source.user_1", "source.user_2", "source.shop"]
        url = "jdbc:mysql://localhost:3306/source"
    }
}

transform {
  TableMerge {
    plugin_input = "customers_mysql_cdc"
    plugin_output = "trans_result"
    
    table_match_regex = "source.user_.*"
    database = "user_db"
    table = "user_all"
  }
}

sink {
  Jdbc {
    plugin_input = "trans_result"
    
    driver="com.mysql.cj.jdbc.Driver"
    url="jdbc:mysql://localhost:3306/sink"
    user="myuser"
    password="mypwd"
    
    generate_sink_sql = true
    database = "${database_name}"
    table = "${table_name}"
    primary_keys = ["${primary_key}"]
    
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

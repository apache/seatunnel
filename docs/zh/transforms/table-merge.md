# 多表合并

> TableMerge：将分库分表或多张来源表合并为统一输出表

## 描述

TableMerge 转换插件用于将分库分表或多张来源表合并为统一输出表。

## 参数

| 参数名   | 类型   | 是否必填 | 默认值 | 说明 |
|:--------:|--------|----------|--------|------|
| database | string | 否       |        | 合并后新的 database 名称 |
| schema   | string | 否       |        | 合并后新的 schema 名称 |
| table    | string | 是       |        | 合并后新的 table 名称 |

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

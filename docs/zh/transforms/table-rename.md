# 表重命名

> TableRename 转换插件

## 描述

TableRename 转换插件用于重命名表名。

## 选项

|          参数           | 类型   | 必选 | 默认值 | 说明                                                                                                    |
|:-----------------------:|--------|------|--------|---------------------------------------------------------------------------------------------------------|
|      convert_case       | string | 否   |        | 字母大小写转换类型，可选 `UPPER`、`LOWER`                                                               |
|         prefix          | string | 否   |        | 追加到表名前的前缀                                                                                      |
|         suffix          | string | 否   |        | 追加到表名后的后缀                                                                                      |
| replacements_with_regex | array  | 否   |        | 正则替换规则数组，元素为包含 `replace_from`、`replace_to` 的映射，用于批量替换表名                      |

## 示例

### 将表名转为大写

```
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
    MySQL-CDC {
        plugin_output = "customers_mysql_cdc"
        
        username = "root"
        password = "123456"
        table-names = ["source.user_shop", "source.user_order"]
        url = "jdbc:mysql://localhost:3306/source"
    }
}

transform {
  TableRename {
    plugin_input = "customers_mysql_cdc"
    plugin_output = "trans_result"
    
    convert_case = "UPPER"
    prefix = "CDC_"
    suffix = "_TABLE"
    replacements_with_regex = [
      {
        replace_from = "user"
        replace_to = "U"
      }
    ]
  }
}

sink {
  Jdbc {
    plugin_input = "trans_result"
    
    driver="oracle.jdbc.OracleDriver"
    url="jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
    user="myuser"
    password="mypwd"
    
    generate_sink_sql = true
    database = "ORCLCDB"
    table = "${database_name}.${table_name}"
    primary_keys = ["${primary_key}"]
    
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### 将表名转为小写

```
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
  Oracle-CDC {
    plugin_output = "customers_oracle_cdc"
    
    url = "jdbc:oracle:thin:@localhost:1521/ORCLCDB"
    username = "dbzuser"
    password = "dbz"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["SOURCE.USER_SHOP", "SOURCE.USER_ORDER"]
  }
}

transform {
  TableRename {
    plugin_input = "customers_oracle_cdc"
    plugin_output = "trans_result"
    
    convert_case = "LOWER"
    prefix = "cdc_"
    suffix = "_table"
    replacements_with_regex = [
      {
        replace_from = "USER"
        replace_to = "u"
      }
    ]
  }
}

sink {
  Jdbc {
    plugin_input = "trans_result"
    
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    
    generate_sink_sql = true
    database = "${schema_name}"
    table = "${table_name}"
    primary_keys = ["${primary_key}"]
    
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```



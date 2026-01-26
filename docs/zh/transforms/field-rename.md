# 字段重命名

> FieldRename 转换插件

## 描述

FieldRename 用于批量重命名字段名。

## 选项

|          参数           | 类型   | 必选 | 默认值 | 说明                                                                                                    |
|:-----------------------:|--------|------|--------|---------------------------------------------------------------------------------------------------------|
|      convert_case       | string | 否   |        | 字母大小写转换类型，可选 `UPPER`、`LOWER`                                                               |
|         prefix          | string | 否   |        | 追加到字段名前的前缀                                                                                    |
|         suffix          | string | 否   |        | 追加到字段名后的后缀                                                                                    |
| replacements_with_regex | array  | 否   |        | 替换规则数组，元素为包含 `replace_from`、`replace_to` 以及可选 `is_regex`（默认 `true`）的映射；当 `is_regex=false` 时，`replace_from` 按字段名精确匹配（全匹配） |
|        specific         | array  | 否   |        | 指定字段重命名规则，元素为包含 `field_name` 和 `target_name` 的映射；命中后会直接重命名并跳过其他规则 |

## 示例

### 将字段名转为大写

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
  FieldRename {
    plugin_input = "customers_mysql_cdc"
    plugin_output = "trans_result"
    
    convert_case = "UPPER"
    prefix = "F_"
    suffix = "_S"
    replacements_with_regex = [
      {
        replace_from = "create_time"
        replace_to = "SOURCE_CREATE_TIME"
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

### 指定字段重命名

```
transform {
  FieldRename {
    plugin_input = "input"
    plugin_output = "output"

    specific = [
      { field_name = "InvoiceNum", target_name = "invoice_num" }
    ]
  }
}
```

### 将字段名转为小写

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
  FieldRename {
    plugin_input = "customers_oracle_cdc"
    plugin_output = "trans_result"
    
    convert_case = "LOWER"
    prefix = "f_"
    suffix = "_s"
    replacements_with_regex = [
      {
        replace_from = "CREATE_TIME"
        replace_to = "source_create_time"
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


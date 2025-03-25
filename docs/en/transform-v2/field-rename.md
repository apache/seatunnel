# FieldRename

> FieldRename transform plugin

## Description

FieldRename transform plugin for rename field name.

## Options

|          name           | type   | required | default value | Description                                                                                                           |
|:-----------------------:|--------|----------|---------------|-----------------------------------------------------------------------------------------------------------------------|
|      convert_case       | string | no       |               | The case conversion type. The options can be `UPPER`, `LOWER`                                                         |
|         prefix          | string | no       |               | The prefix to be added to the field name                                                                              |
|         suffix          | string | no       |               | The suffix to be added to the field name                                                                              |
| replacements_with_regex | array  | no       |               | The array of replacement rules with regex. The replacement rule is a map with `replace_from` and `replace_to` fields. |

## Examples

### Convert field to uppercase

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
        base-url = "jdbc:mysql://localhost:3306/source"
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

### Convert field name to lowercase

```
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
  Oracle-CDC {
    plugin_output = "customers_oracle_cdc"
    
    base-url = "jdbc:oracle:thin:@localhost:1521/ORCLCDB"
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
# TableMerge

> TableMerge transform plugin

## Description

TableMerge transform plugin for merge sharding-tables.

## Options

|   name   | type   | required | default value | Description               |
|:--------:|--------|----------|---------------|---------------------------|
| database | string | no       |               | Specify new database name |
|  schema  | string | no       |               | Specify new schema name   |
|  table   | string | yes      |               | Specify new table name    |

## Examples

### Merge sharding-tables

`
```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
    MySQL-CDC {
        plugin_output = "customers_mysql_cdc"
        
        username = "root"
        password = "123456"
        table-names = ["source.user_1", "source.user_2", "source.shop"]
        base-url = "jdbc:mysql://localhost:3306/source"
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

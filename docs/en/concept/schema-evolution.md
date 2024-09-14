# Schema evolution
Schema Evolution means that the schema of a data table can be changed and the data synchronization task can automatically adapt to the changes of the new table structure without any other operations.
Now we only support the operation about `add column`、`drop column`、`rename column` and `modify column` of the table in CDC source. This feature is only support zeta engine at now.

## Supported connectors

### Source
[Mysql-CDC](https://github.com/apache/seatunnel/blob/dev/docs/en/connector-v2/source/MySQL-CDC.md)

### Sink
[Jdbc-Mysql](https://github.com/apache/seatunnel/blob/dev/docs/en/connector-v2/sink/Jdbc.md)

Note: The schema evolution is not support the transform at now. 

## Enable schema evolution
Schema evolution is disabled by default in CDC source. You need configure `debezium.include.schema.changes = true` which is only supported in MySQL-CDC to enable it.

## Examples

### Mysql-CDC -> Jdbc-Mysql
```
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    debezium = {
      include.schema.changes = true
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    generate_sink_sql = true
    database = shop
    table = mysql_cdc_e2e_sink_table_with_schema_change_exactly_once
    primary_keys = ["id"]
    is_exactly_once = true
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
  }
}
```

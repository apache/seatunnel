# 模式演进
模式演进是指数据表的Schema可以改变，数据同步任务可以自动适应新的表结构的变化而无需其他操作。
现在我们只支持对CDC源中的表进行“添加列”、“删除列”、“重命名列”和“修改列”的操作。目前这个功能只支持zeta引擎。

## 已支持的连接器

### 源
[Mysql-CDC](https://github.com/apache/seatunnel/blob/dev/docs/en/connector-v2/source/MySQL-CDC.md)

### 目标
[Jdbc-Mysql](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Jdbc.md)

注意: 目前模式演进不支持transform.


## 启用Schema evolution功能
在CDC源连接器中模式演进默认是关闭的。你需要在CDC连接器中配置`debezium.include.schema.changes = true`来启用它。

## 示例

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

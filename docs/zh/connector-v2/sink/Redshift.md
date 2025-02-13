# Redshift

> JDBC Redshift 接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [更改数据捕获](../../concept/connector-v2-features.md)

> 使用 `Xa transactions` 确保 `exactly-once`. 因此，数据库只支持 `exactly-once` 
> 即支持 `Xa transactions`. 您可以设置 `is_exactly_once=true` 来启用它.

## 描述

通过jdbc写入数据. 支持批处理模式和流模式，支持并发写入，只支持一次语义 (使用 XA transaction guarantee).

## 支持的数据源列表

| 数据源 |                    支持版本                    | 驱动                              |                   url                   | maven                                                                        |
|------------|----------------------------------------------------------|---------------------------------|-----------------------------------------|------------------------------------------------------------------------------|
| redshift   | 不同的依赖版本有不同的驱动程序类. | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [下载](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## 数据库相关性

### 适用于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/plugins/`.

### 适用于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/lib/`.

## 数据类型映射

| SeaTunnel 数据类型          | Redshift 数据类型 |
|-------------------------|--------------------|
| BOOLEAN                 | BOOLEAN            |
| TINYINT<br/> SMALLINT   | SMALLINT           |
| INT                     | INTEGER            |
| BIGINT                  | BIGINT             |
| FLOAT                   | REAL               |
| DOUBLE                  | DOUBLE PRECISION   |
| DECIMAL                 | NUMERIC            |
| STRING(<=65535)         | CHARACTER VARYING  |
| STRING(>65535)          | SUPER              |
| BYTES                   | BINARY VARYING     |
| TIME                    | TIME               |
| TIMESTAMP               | TIMESTAMP          |
| MAP<br/> ARRAY<br/> ROW | SUPER              |

## 任务示例

### 简单示例:

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        user = "myUser"
        password = "myPassword"
        
        generate_sink_sql = true
        schema = "public"
        table = "sink_table"
    }
}
```

### CDC(更改数据捕获) 事件

> 我们也支持CDC更改数据。在这种情况下，您需要配置数据库、表和主键.

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        user = "myUser"
        password = "mypassword"
        
        generate_sink_sql = true
        schema = "public"
        table = "sink_table"
        
        # config update/delete primary keys
        primary_keys = ["id","name"]
    }
}
```


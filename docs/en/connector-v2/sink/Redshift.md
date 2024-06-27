# Redshift

> JDBC Redshift sink Connector

## Support those engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Supported DataSource list

| datasource |                    supported versions                    |             driver              |                   url                   |                                       maven                                        |
|------------|----------------------------------------------------------|---------------------------------|-----------------------------------------|------------------------------------------------------------------------------------|
| redshift   | Different dependency version has different driver class. | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [Download](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## Database dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Data Type Mapping

|   SeaTunnel Data type   | Redshift Data type |
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

## Task Example

### Simple:

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

### CDC(Change data capture) event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

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


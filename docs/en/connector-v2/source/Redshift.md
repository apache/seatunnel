# Redshift

> JDBC Redshift Source Connector

## Description

Read external data source data through JDBC.

## Support those engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Supported DataSource list

| datasource |                    supported versions                    |             driver              |                   url                   |                                       maven                                        |
|------------|----------------------------------------------------------|---------------------------------|-----------------------------------------|------------------------------------------------------------------------------------|
| redshift   | Different dependency version has different driver class. | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [Download](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Redshift datasource: cp RedshiftJDBC42-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                                                Redshift Data type                                                 |                                                                 Seatunnel Data type                                                                 |
|-------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| SMALLINT<br />INT2                                                                                                | SHORT                                                                                                                                               |
| INTEGER<br />INT<br />INT4                                                                                        | INT                                                                                                                                                 |
| BIGINT<br />INT8<br />OID                                                                                         | LONG                                                                                                                                                |
| DECIMAL<br />NUMERIC                                                                                              | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| REAL<br />FLOAT4                                                                                                  | FLOAT                                                                                                                                               |
| DOUBLE_PRECISION<br />FLOAT8<br />FLOAT                                                                           | DOUBLE                                                                                                                                              |
| BOOLEAN<br />BOOL                                                                                                 | BOOLEAN                                                                                                                                             |
| CHAR<br />CHARACTER<br />NCHAR<br />BPCHAR<br />VARCHAR<br />CHARACTER_VARYING<br />NVARCHAR<br />TEXT<br />SUPER | STRING                                                                                                                                              |
| VARBYTE<br />BINARY_VARYING                                                                                       | BYTES                                                                                                                                               |
| TIME<br />TIME_WITH_TIME_ZONE<br />TIMETZ                                                                         | LOCALTIME                                                                                                                                           |
| TIMESTAMP<br />TIMESTAMP_WITH_OUT_TIME_ZONE<br />TIMESTAMPTZ                                                      | LOCALDATETIME                                                                                                                                       |

## Example

### Simple:

> This example queries type_bin 'table' 16 data in your test "database" in single parallel and queries all of its fields. You can also specify which fields to query for final output to the console.

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:redshift://localhost:5439/dev"
        driver = "com.amazon.redshift.jdbc.Driver"
        user = "root"
        password = "123456"
        
        table_path = "public.table2"
        # Use query filetr rows & columns
        query = "select id, name from public.table2 where id > 100"
        
        #split.size = 8096
        #split.even-distribution.factor.upper-bound = 100
        #split.even-distribution.factor.lower-bound = 0.05
        #split.sample-sharding.threshold = 1000
        #split.inverse-sampling.rate = 1000
    }
}

sink {
    Console {}
}
```

### Multiple table read:

***Configuring `table_list` will turn on auto split, you can configure `split.*` to adjust the split strategy***

```hocon
env {
  job.mode = "BATCH"
  parallelism = 2
}
source {
  Jdbc {
    url = "jdbc:redshift://localhost:5439/dev"
    driver = "com.amazon.redshift.jdbc.Driver"
    user = "root"
    password = "123456"

    table_list = [
      {
        table_path = "public.table1"
      },
      {
        table_path = "public.table2"
        # Use query filetr rows & columns
        query = "select id, name from public.table2 where id > 100"
      }
    ]
    #split.size = 8096
    #split.even-distribution.factor.upper-bound = 100
    #split.even-distribution.factor.lower-bound = 0.05
    #split.sample-sharding.threshold = 1000
    #split.inverse-sampling.rate = 1000
  }
}

sink {
  Console {}
}
```


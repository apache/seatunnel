# Phoenix

> Phoenix source connector

## Description
Read Phoenix data through [Jdbc connector](Jdbc.md).
Support Batch mode and Streaming mode. The tested Phoenix version is 4.xx and 5.xx
On the underlying implementation, through the jdbc driver of Phoenix, execute the upsert statement to write data to HBase.
Two ways of connecting Phoenix with Java JDBC. One is to connect to zookeeper through JDBC, and the other is to connect to queryserver through JDBC thin client.

> Tips: By default, the (thin) driver jar is used. If you want to use the (thick) driver  or other versions of Phoenix (thin) driver, you need to recompile the jdbc connector module

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

### driver [string]
if you use phoenix (thick) driver the value is `org.apache.phoenix.jdbc.PhoenixDriver` or you use (thin) driver the value is `org.apache.phoenix.queryserver.client.Driver`

### url [string]
if you use phoenix (thick) driver the value is `jdbc:phoenix:localhost:2182/hbase` or you use (thin) driver the value is `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example
use thick client drive
```
    Jdbc {
        driver = org.apache.phoenix.jdbc.PhoenixDriver
        url = "jdbc:phoenix:localhost:2182/hbase"
        query = "select age, name from test.source"
    }

```

use thin client drive
```
    Jdbc {
        driver = org.apache.phoenix.queryserver.client.Driver
        url = "jdbc:phoenix:thin:url=http://spark_e2e_phoenix_sink:8765;serialization=PROTOBUF"
        query = "select age, name from test.source"
    }
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Phoenix Source Connector

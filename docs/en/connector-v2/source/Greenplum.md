# Greenplum

> Greenplum source connector

## Description

Read Greenplum data through [Jdbc connector](Jdbc.md).

## Key features

- [x] [batch](key-features.md)
- [ ] [stream](key-features.md)
- [ ] [exactly-once](key-features.md)
- [x] [schema projection](key-features.md) 

supports query SQL and can achieve projection effect.

- [x] [parallelism](key-features.md)

:::tip

Optional jdbc drivers:
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

Warn: for license compliance, if you use `GreenplumDriver` the have to provide Greenplum JDBC driver yourself, e.g. copy greenplum-xxx.jar to $SEATNUNNEL_HOME/lib for Standalone.

:::
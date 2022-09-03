# Greenplum

> Greenplum source connector

## Description

Read Greenplum data through [Jdbc connector](Jdbc.md).

## Key features

- [x] batch
- [ ] stream
- [ ] exactly-once
- [x] schema projection   

supports query SQL and can achieve projection effect.

- [x] parallelism

:::tip

Optional jdbc drivers:
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

Warn: for license compliance, if you use `GreenplumDriver` the have to provide Greenplum JDBC driver yourself, e.g. copy greenplum-xxx.jar to $SEATNUNNEL_HOME/lib for Standalone.

:::
# Greenplum

> Greenplum source connector

## Description

Read Greenplum data through [Jdbc connector](Jdbc.md).

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md) 

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

:::tip

Optional jdbc drivers:
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

Warn: for license compliance, if you use `GreenplumDriver` the have to provide Greenplum JDBC driver yourself, e.g. copy greenplum-xxx.jar to $SEATNUNNEL_HOME/lib for Standalone.

:::

## Options

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.


## Changelog

### 2.2.0-beta 2022-09-26

- Add Greenplum Source Connector

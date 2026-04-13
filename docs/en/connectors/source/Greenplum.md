import ChangeLog from '../changelog/connector-jdbc.md';

# Greenplum

> Greenplum source connector

## Description

Read Greenplum data through [Jdbc connector](Jdbc.md).

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip

Optional jdbc drivers:
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

Warn: for license compliance, if you use `GreenplumDriver` the have to provide Greenplum JDBC driver yourself, e.g. copy greenplum-xxx.jar to $SEATUNNEL_HOME/lib for Standalone.

:::

## Options

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Changelog

<ChangeLog />


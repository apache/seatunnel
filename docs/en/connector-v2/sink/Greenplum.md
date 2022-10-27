# Greenplum

> Greenplum sink connector

## Description

Write data to Greenplum using [Jdbc connector](Jdbc.md).

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

:::tip

Not support exactly-once semantics (XA transaction is not yet supported in Greenplum database).

:::

## Options

### driver [string]

Optional jdbc drivers:
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

Warn: for license compliance, if you use `GreenplumDriver` the have to provide Greenplum JDBC driver yourself, e.g. copy greenplum-xxx.jar to $SEATNUNNEL_HOME/lib for Standalone.

### url [string]

The URL of the JDBC connection. if you use postgresql driver the value is `jdbc:postgresql://${yous_host}:${yous_port}/${yous_database}`, or you use greenplum driver the value is `jdbc:pivotal:greenplum://${yous_host}:${yous_port};DatabaseName=${yous_database}`

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Changelog

### 2.2.0-beta 2022-09-26

- Add Greenplum Sink Connector

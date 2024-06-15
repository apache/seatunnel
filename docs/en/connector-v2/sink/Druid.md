# Druid

> Druid sink connector

## Description

Write data to Druid

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Data Type Mapping

| Druid Data Type | SeaTunnel Data Type |
|-----------------|---------------------|
| LONG            | TINYINT             |
| LONG            | SMALLINT            |
| LONG            | INT                 |
| LONG            | BIGINT              |
| FLOAT           | FLOAT               |
| DOUBLE          | DOUBLE              |
| DOUBLE          | DECIMAL             |
| STRING          | STRING              |
| STRING          | BOOLEAN             |
| STRING          | TIMESTAMP           |

## Options

|      name      |  type  | required | default value |
|----------------|--------|----------|---------------|
| coordinatorUrl | string | yes      | -             |
| datasource     | string | yes      | -             |
| batchSize      | int    | no       | 10000         |
| common-options |        | no       | -             |

### coordinatorUrl [string]

The coordinatorUrl host and port of Druid, example: "myHost:8888"

### datasource [string]

The datasource name you want to write, example: "seatunnel"

### batchSize [int]

The number of rows flushed to Druid per batch. Default value is `1024`.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

```hocon
Druid {
  coordinatorUrl = "testHost:8888"
  datasource = "seatunnel"
}
```

## Changelog

### next version

- Add Druid sink connector


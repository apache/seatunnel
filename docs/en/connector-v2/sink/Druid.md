# Druid

> Druid sink connector

## Description

Write data to Druid

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Data Type Mapping

| SeaTunnel Data Type | Druid Data Type |
|---------------------|-----------------|
| TINYINT             | LONG            |
| SMALLINT            | LONG            |
| INT                 | LONG            |
| BIGINT              | LONG            |
| FLOAT               | FLOAT           |
| DOUBLE              | DOUBLE          |
| DECIMAL             | DOUBLE          |
| STRING              | STRING          |
| BOOLEAN             | STRING          |
| TIMESTAMP           | STRING          |

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

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details

## Example

Simple example:

```hocon
sink {
  Druid {
    coordinatorUrl = "testHost:8888"
    datasource = "seatunnel"
  }
}
```

Use placeholders get upstream table metadata example:

```hocon
sink {
  Druid {
    coordinatorUrl = "testHost:8888"
    datasource = "${table_name}_test"
  }
}
```

## Changelog

### next version

- Add Druid sink connector


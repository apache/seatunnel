import ChangeLog from '../changelog/connector-neo4j.md';

# Neo4j

> Neo4j source connector

## Description

The Neo4j source connector reads data from Neo4j by running a Cypher query and mapping
the returned fields to a SeaTunnel schema.

`neo4j-java-driver` version: 4.4.9

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

| Neo4j Value Type | SeaTunnel Data Type |
|------------------|---------------------|
| String           | STRING              |
| Boolean          | BOOLEAN             |
| Integer          | INT / BIGINT        |
| Float            | FLOAT / DOUBLE      |
| ByteArray        | BYTES               |
| Date             | DATE                |
| LocalTime        | TIME                |
| LocalDateTime    | TIMESTAMP           |
| List             | ARRAY               |
| Map              | MAP                 |
| Null             | NULL                |

## Source Options

| Name                       | Type   | Required | Default | Description                                                                                                                                       |
|----------------------------|--------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                        | String | Yes      | -       | Neo4j connection URI, for example `neo4j://localhost:7687` or `bolt://localhost:7687`.                                                            |
| username                   | String | No       | -       | Neo4j username. Use it together with `password`. One of `username`, `bearer_token`, or `kerberos_ticket` must be configured.                      |
| password                   | String | No       | -       | Neo4j password. Required when `username` is configured.                                                                                            |
| bearer_token               | String | No       | -       | Bearer token used for Neo4j authentication.                                                                                                       |
| kerberos_ticket            | String | No       | -       | Kerberos ticket used for Neo4j authentication.                                                                                                    |
| database                   | String | Yes      | -       | Neo4j database name.                                                                                                                              |
| query                      | String | Yes      | -       | Cypher query used to read data. The fields returned by this query must match `schema.fields`.                                                     |
| schema                     | Object | Yes      | -       | SeaTunnel schema of the query result. Configure it under `schema.fields`.                                                                         |
| max_transaction_retry_time | Long   | No       | 30      | Maximum transaction retry time, in seconds.                                                                                                       |
| max_connection_timeout     | Long   | No       | 30      | Maximum time to wait for a TCP connection to be established, in seconds.                                                                          |

## Notes

- Use exactly one authentication method: username/password, bearer token, or Kerberos ticket.
- `query` controls which fields are returned. `schema.fields` must list the returned field names and their SeaTunnel types.
- Returned field names can contain dots, such as `t.string`, when the Cypher query returns properties from a node.
- `MAP` fields must use `STRING` keys, for example `MAP<STRING, INT>`.
- Neo4j integer and floating-point values are converted according to the SeaTunnel type declared in `schema.fields`. Use `BIGINT`/`DOUBLE` when the value may exceed the range of `INT`/`FLOAT`.

## Task Example

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"

    max_transaction_retry_time = 1
    max_connection_timeout = 1

    query = "MATCH (t:Test) WITH *, t{.int} AS _map RETURN t.string, t.boolean, t.long, t.double, t.byteArray, t.date, t.localDateTime, _map, t.list, t.int, t.float"

    schema {
      fields {
        t.string = STRING
        t.boolean = BOOLEAN
        t.long = BIGINT
        t.double = DOUBLE
        t.null = NULL
        t.byteArray = BYTES
        t.date = DATE
        t.localDateTime = TIMESTAMP
        _map = "MAP<STRING, INT>"
        t.list = "ARRAY<INT>"
        t.int = INT
        t.float = FLOAT
      }
    }
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />

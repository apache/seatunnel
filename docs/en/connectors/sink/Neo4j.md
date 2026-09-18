import ChangeLog from '../changelog/connector-neo4j.md';

# Neo4j

> Neo4j sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The Neo4j sink connector writes SeaTunnel rows to Neo4j by running a Cypher statement.
It supports one-row-at-a-time writes and batch writes with Cypher `UNWIND`.

`neo4j-java-driver` version: 4.4.9

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name                       | Type    | Required | Default    | Description                                                                                                                                          |
|----------------------------|---------|----------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                        | String  | Yes      | -          | Neo4j connection URI, for example `neo4j://localhost:7687` or `bolt://localhost:7687`.                                                               |
| username                   | String  | No       | -          | Neo4j username. Use it together with `password`. One of `username`, `bearer_token`, or `kerberos_ticket` must be configured.                         |
| password                   | String  | No       | -          | Neo4j password. Required when `username` is configured.                                                                                               |
| bearer_token               | String  | No       | -          | Bearer token used for Neo4j authentication.                                                                                                          |
| kerberos_ticket            | String  | No       | -          | Kerberos ticket used for Neo4j authentication.                                                                                                       |
| database                   | String  | Yes      | -          | Neo4j database name.                                                                                                                                 |
| query                      | String  | Yes      | -          | Cypher statement used to write data. In `ONE_BY_ONE` mode, use placeholders such as `$name`; in `BATCH` mode, use `UNWIND $batch AS row`.            |
| queryParamPosition         | Object  | ONE_BY_ONE only | -          | Mapping between Cypher parameter names and input row field positions. Required when `write_mode = "ONE_BY_ONE"`.                                    |
| max_batch_size             | Integer | No       | 500        | Maximum number of rows written in one transaction when `write_mode = "BATCH"`. Must be greater than 0.                                                |
| write_mode                 | String  | No       | ONE_BY_ONE | Write mode. Supported values are `ONE_BY_ONE` and `BATCH`.                                                                                            |
| max_transaction_retry_time | Long    | No       | 30         | Maximum transaction retry time, in seconds.                                                                                                          |
| max_connection_timeout     | Long    | No       | 30         | Maximum time to wait for a TCP connection to be established, in seconds.                                                                             |
| common-options             | config  | No       | -          | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md).                                                            |

## Notes

- Configure at least one authentication method: username/password, bearer token, or Kerberos ticket. If several are configured, username/password takes precedence, followed by bearer token and Kerberos ticket.
- In `ONE_BY_ONE` mode, `queryParamPosition` maps each Cypher placeholder to a field position in the input row.
- In `BATCH` mode, the query should use `UNWIND $batch AS row`. The connector passes the rows through the `batch` variable.
- Field positions in `queryParamPosition` start from `0`, following the input schema field order.
- In `BATCH` mode, each `row` entry uses the input field names, so the names referenced in the Cypher statement must match the upstream schema.

## Write One Row At A Time

```bash
sink {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"

    max_transaction_retry_time = 10
    max_connection_timeout = 10

    query = "CREATE (a:Person {name: $name, age: $age})"
    queryParamPosition = {
      name = 0
      age = 1
    }
  }
}
```

## Write In Batches

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    row.num = 1000
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"

    write_mode = "BATCH"
    max_batch_size = 500
    max_transaction_retry_time = 3
    max_connection_timeout = 10

    query = "UNWIND $batch AS row CREATE (n:BatchLabel) SET n.name = row.name, n.age = row.age"
  }
}
```

## Changelog

<ChangeLog />

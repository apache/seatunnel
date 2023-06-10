# Neo4j

> Neo4j sink connector

## Description

Write data to Neo4j.

`neo4j-java-driver` version 4.4.9

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|            name            |  type   | required | default value |
|----------------------------|---------|----------|---------------|
| uri                        | String  | Yes      | -             |
| username                   | String  | No       | -             |
| password                   | String  | No       | -             |
| max_batch_size             | Integer | No       | -             |
| write_mode                 | String  | No       | OneByOne      |
| bearer_token               | String  | No       | -             |
| kerberos_ticket            | String  | No       | -             |
| database                   | String  | Yes      | -             |
| query                      | String  | Yes      | -             |
| queryParamPosition         | Object  | Yes      | -             |
| max_transaction_retry_time | Long    | No       | 30            |
| max_connection_timeout     | Long    | No       | 30            |
| common-options             | config  | no       | -             |

### uri [string]

The URI of the Neo4j database. Refer to a case: `neo4j://localhost:7687`

### username [string]

username of the Neo4j

### password [string]

password of the Neo4j. required if `username` is provided

### max_batch_size[Integer]

max_batch_size refers to the maximum number of data entries that can be written in a single transaction when writing to a database.

### write_mode

The default value is oneByOne, or set it to "Batch" if you want to have the ability to write in batches

```cypher
unwind $ttt as row create (n:Label) set n.name = row.name,n.age = rw.age
```

"ttt" represents a batch of data.,"ttt" can be any arbitrary string as long as it matches the configured "batch_data_variable".

### bearer_token [string]

base64 encoded bearer token of the Neo4j. for Auth.

### kerberos_ticket [string]

base64 encoded kerberos ticket of the Neo4j. for Auth.

### database [string]

database name.

### query [string]

Query statement. contain parameter placeholders that are substituted with the corresponding values at runtime

### queryParamPosition [object]

position mapping information for query parameters.

key name is parameter placeholder name.

associated value is position of field in input data row.

### max_transaction_retry_time [long]

maximum transaction retry time(seconds). transaction fail if exceeded

### max_connection_timeout [long]

The maximum amount of time to wait for a TCP connection to be established (seconds)

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## WriteOneByOneExample

```
sink {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "1234"
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

## WriteBatchExample
> The unwind keyword provided by cypher supports batch writing, and the default variable for a batch of data is batch. If you write a batch write statement, then you should declare cypher:unwind $batch as row to do someting
```
sink {
  Neo4j {
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "neo4j"
    database = "neo4j"
    max_batch_size = 1000
    write_mode = "BATCH"

    max_transaction_retry_time = 3
    max_connection_timeout = 10

    query = "unwind $batch as row  create(n:MyLabel) set n.name = row.name,n.age = row.age"

  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Neo4j Sink Connector

### issue ##4835

- Sink supports batch write


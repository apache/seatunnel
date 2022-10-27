# Neo4j

> Neo4j sink connector

## Description

Write data to Neo4j. 

`neo4j-java-driver` version 4.4.9

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name                       | type   | required | default value |
|----------------------------|--------|----------|---------------|
| uri                        | String | Yes      | -             |
| username                   | String | No       | -             |
| password                   | String | No       | -             |
| bearer_token               | String | No       | -             |
| kerberos_ticket            | String | No       | -             |
| database                   | String | Yes      | -             |
| query                      | String | Yes      | -             |
| queryParamPosition         | Object | Yes      | -             |
| max_transaction_retry_time | Long   | No       | 30            |
| max_connection_timeout     | Long   | No       | 30            |
| common-options             |        | no       | -             |

### uri [string]
The URI of the Neo4j database. Refer to a case: `neo4j://localhost:7687`

### username [string]
username of the Neo4j

### password [string]
password of the Neo4j. required if `username` is provided

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


## Example
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

## Changelog

### 2.2.0-beta 2022-09-26

- Add Neo4j Sink Connector

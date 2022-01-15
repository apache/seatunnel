# Source plugin: Cassandra

### Description

Read data from Cassandra.

### Env
| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [spark.cassandra.connection.host](#spark.cassandra.connection.host-string)       | string | yes      | -             |

##### spark.cassandra.connection.host [string]

Cassandra connection host

##### other

Refer to [spark-cassandra-connector-options](https://github.com/datastax/spark-cassandra-connector/blob/b2.4/doc/reference.md) for configurations.

### Options

| name             | type   | required | default value |
| --------------   | ------ | -------- | ------------- |
| [table](#table-string)            | string | yes      | -             |
| [keyspace](#keyspace-string)         | string | yes      | -             |
| [cluster](#cluster-string)          | string | no       | default       |
| [pushdown](#pushdown-string)         | string | no       | true         |

##### table [string]

The Cassandra table to connect to

##### keyspace [string]

The keyspace where table is looked for 

##### cluster [string]

The group of the Cluster Level Settings to inherit

##### pushdown [string]

Enables pushing down predicates to Cassandra when applicable

##### others

Refer to [Spark-Cassandra-Connector Options](https://github.com/datastax/spark-cassandra-connector/blob/b2.4/doc/14_data_frames.md)
### Example

```bash
cassandra {
    table = "t2"
    keyspace = "excelsior"
    result_table_name = "test"
}
```


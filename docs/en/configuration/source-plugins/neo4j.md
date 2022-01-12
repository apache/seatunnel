# Source plugin: Neo4j [Spark]

## Description

Read data from Neo4j.

Neo4j Connector for Apache Spark allows you to read data from Neo4j in 3 different ways: by node labels, by relationship name, and by direct Cypher query.

for detail neo4j config message please visit [neo4j doc](#https://neo4j.com/docs/spark/current/reading/) 

## Options

| name                                                                     | type   | required | default value |
| -------------------------------------------------------------------------| ------ | -------- | ------------- |
| [result_table_name](#result_table_name-string)                           | string | yes      | -             |
| [authentication_type](#authentication_type-string)                       | string | no       | -             |
| [authentication_basic_username](#authentication_basic_username-string)   | string | no       | -             |
| [authentication_basic_password](#authentication_basic_password-string)   | string | no       | -             |
| [url](#url-string)                                                       | string | yes      | -             |
| [query](#query-string)                                                   | string | yes*     | -             |
| [labels](#labels-string)                                                 | string | yes*     | -             |
| [relationship](#relationship-string)                                     | string | yes*     | -             |
| [schema_flatten_limit](#schema_flatten_limit-string)                     | string | no       | -             |
| [schema_strategy](#schema_strategy-string)                               | string | no       | -             |
| [pushdown_filters_enabled](#pushdown_filters_enabled-string)             | string | no       | -             |
| [pushdown_columns_enabled](#pushdown_columns_enabled-string)             | string | no       | -             |
| [partitions](#partitions-string)                                         | string | no       | -             |
| [query_count](#query_count-string)                                       | string | no       | -             |
| [relationship_nodes_map](#relationship_nodes_map-string)                 | string | no       | -             |
| [relationship_source_labels](#relationship_source_labels-string)         | string | Yes      | -             |
| [relationship_target_labels](#relationship_target_labels-string)         | string | Yes      | -             |

### result_table_name [string]

result table name

### authentication_type [string]

authentication type

### authentication_basic_username [string]

username

### authentication_basic_password [string]

password

### url [string]
url 

### query [string]

Cypher query to read the data

### labels [string]

List of node labels separated by :. The first label will be the primary label

### relationship [string]

Name of a relationship

### schema_flatten_limit [string]
Number of records to be used to create the Schema (only if APOC are not installed)

### schema_strategy [string]

Strategy used by the connector in order to compute the Schema definition for the Dataset. Possibile values are string, sample. When string it coerces all the properties to String otherwise it will try to sample the Neo4j’s dataset.

### pushdown_filters_enabled [string]

Enable or disable the Push Down Filters support

### pushdown_columns_enabled [string]

Enable or disable the Push Down Column support

### partitions [string]

This defines the parallelization level while pulling data from Neo4j.

Note: as more parallelization does not mean more performances so please tune wisely in according to your Neo4j installation.

### query_count [string]

Query count, used only in combination with query option, it’s a query that returns a count field like the following:

MATCH (p:Person)-[r:BOUGHT]->(pr:Product)
WHERE pr.name = 'An Awesome Product'
RETURN count(p) AS count
or a simple number that represents the amount of records returned by query. Consider that the number passed by this value represent the volume of the data pulled of Neo4j, so please use it carefully.

### relationship_nodes_map [string]


If true return source and target nodes as Map<String, String>, otherwise we flatten the properties by returning every single node property as column prefixed by source or target

### relationship_source_labels [string]

List of source node Labels separated by :

### relationship_target_labels [string]

List of target node Labels separated by :

## Example

```bash
   Neo4j {
      result_table_name = "test"
      authentication_type = "basic"
      authentication_basic_username = "test"
      authentication_basic_password = "test"
      url = "bolt://localhost:7687"
      labels = "Person"
      #query = "MATCH (n1)-[r]->(n2) RETURN r, n1, n2 "
   }
```

> The returned table is a data table in which both fields are strings

+----+--------+--------------------+----+
|<id>|<labels>|name                |born|
+----+--------+--------------------+----+
|1   |[Person]|Keanu Reeves        |1964|
|2   |[Person]|Carrie-Anne Moss    |1967|
|3   |[Person]|Laurence Fishburne  |1961|
|4   |[Person]|Hugo Weaving        |1960|
|5   |[Person]|Andy Wachowski      |1967|
|6   |[Person]|Lana Wachowski      |1965|
|7   |[Person]|Joel Silver         |1952|
|8   |[Person]|Emil Eifrem         |1978|
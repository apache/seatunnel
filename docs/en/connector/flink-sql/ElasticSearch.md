# Flink SQL ElasticSearch Connector

> ElasticSearch connector based flink sql

## Description
With elasticsearch connector, you can use the Flink SQL to write data into ElasticSearch.


## Usage
Let us have a brief example to show how to use the connector.

### 1. Elastic prepare
Please refer to the [Elastic Doc](https://www.elastic.co/guide/index.html) to prepare elastic environment.

### 2. prepare seatunnel configuration
ElasticSearch provide different connectors for different version:
* version 6.x: flink-sql-connector-elasticsearch6
* version 7.x: flink-sql-connector-elasticsearch7

Here is a simple example of seatunnel configuration.
```sql
SET table.dml-sync = true;

CREATE TABLE events (
    id INT,
    name STRING
) WITH (
    'connector' = 'datagen'
);

CREATE TABLE es_sink (
    id INT,
    name STRING
) WITH (
    'connector' = 'elasticsearch-7', -- or 'elasticsearch-6'
    'hosts' = 'http://localhost:9200',
    'index' = 'users'
);

INSERT INTO es_sink SELECT * FROM events;
```

### 3. start Flink SQL job
Execute the following command in seatunnel home path to start the Flink SQL job.
```bash
$ bin/start-seatunnel-sql.sh -c config/elasticsearch.sql.conf
```

### 4. verify result
Verify result from elasticsearch.

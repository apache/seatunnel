# Flink SQL HBase Connector

## Description
With hbase connector, you can use the Flink SQL to read data from HBase and write data into HBase.


## Usage
Let us have a brief example to show how to use the connector.

### 1. kafka prepare
Please refer to the [HBase Guide](https://hbase.apache.org/book.html) to prepare HBase environment.

### 2. prepare seatunnel configuration
HBase provide different connectors for different version:
* version 1.2.x: flink-sql-connector-hbase-1.2
* version 2.2.x: flink-sql-connector-hbase-2.2

Here is a simple example of seatunnel configuration.
```sql
SET table.dml-sync = true;

-- register the HBase table 'mytable' in Flink SQL
CREATE TABLE hTable (
    rowkey INT,
    family1 ROW<q1 INT>,
    family2 ROW<q2 STRING, q3 BIGINT>,
    family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-1.4',  -- or 'hbase-2.2'
    'table-name' = 'mytable',
    'zookeeper.quorum' = 'localhost:2181'
);

-- use ROW(...) construction function construct column families and write data into the HBase table.
-- assuming the schema of "T" is [rowkey, f1q1, f2q2, f2q3, f3q4, f3q5, f3q6]
INSERT INTO hTable
SELECT rowkey, ROW(f1q1), ROW(f2q2, f2q3), ROW(f3q4, f3q5, f3q6) FROM T;
```

### 3. start Flink SQL job
Execute the following command in seatunnel home path to start the Flink SQL job.
```bash
$ bin/start-seatunnel-sql.sh -c config/hbase.sql.conf
```

### 4. verify result
Verify result from hbase.

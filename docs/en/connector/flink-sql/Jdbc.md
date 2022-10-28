# Flink SQL JDBC Connector

> JDBC connector based flink sql

## Description

We can use the Flink SQL JDBC Connector to connect to a JDBC database. Refer to the [Flink SQL JDBC Connector](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/jdbc/index.html) for more information.


## Usage

### 1. download driver
A driver dependency is also required to connect to a specified database. Here are drivers currently supported:

| Driver     | Group Id	         | Artifact Id	        | JAR           |
|------------|-------------------|----------------------|---------------|
| MySQL	     | mysql	         | mysql-connector-java | [Download](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/) |
| PostgreSQL | org.postgresql	 | postgresql	        | [Download](https://jdbc.postgresql.org/download/) |
| Derby	     | org.apache.derby	 | derby	            | [Download](http://db.apache.org/derby/derby_downloads.html) |

After downloading the driver jars, you need to place the jars into $FLINK_HOME/lib/.

### 2. prepare data
Start mysql server locally, and create a database named "test" and a table named "test_table" in the database.

The table "test_table" could be created by the following SQL:
```sql
CREATE TABLE IF NOT EXISTS `test_table`(
   `id` INT UNSIGNED AUTO_INCREMENT,
   `name` VARCHAR(100) NOT NULL,
   PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

Insert some data into the table "test_table".

### 3. seatunnel config 
Prepare a seatunnel config file with the following content:
```sql
SET table.dml-sync = true;

CREATE TABLE test (
  id BIGINT,
  name STRING
) WITH (
'connector'='jdbc',
  'url' = 'jdbc:mysql://localhost:3306/test',
  'table-name' = 'test_table',
  'username' = '<replace with your username>',
  'password' = '<replace with your password>'
);

CREATE TABLE print_table (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'print',
  'sink.parallelism' = '1'
);

INSERT INTO print_table SELECT * FROM test;
```

### 4. run job
```bash
./bin/start-seatunnel-sql.sh --config <path/to/your/config>
```

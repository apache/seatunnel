# Flink SQL MySQL CDC Connector

## Description

We can use the Flink Mysql CDC Connector allows for reading snapshot data and incremental data from MySQL database. Refer to the [Flink CDC MySQL CDC Connector](https://ververica.github.io/flink-cdc-connectors/release-2.1/content/connectors/mysql-cdc.html#connector-options) for more information.

## Usage

### 1. Flink MySQL CDC Version

flink-sql-connector-mysql-cdc-2.1.1

### 2. prepare data

Start mysql server locally, and create a database named "test" and a table named "test_table" in the database.

The table "test_table" could be created by the following SQL:

```sql
CREATE TABLE IF NOT EXISTS `test_table`(
   `id` INT UNSIGNED AUTO_INCREMENT,
   `class_id` INT,
   `student_id` INT,
   `course` VARCHAR(100),
   PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

Insert some data into the table "test_table".

### 3. seatunnel config 

Prepare a seatunnel config file with the following content:

```sql
SET table.dml-sync = true;
SET sql-client.verbose = true;
SET sql-client.execution.result-mode = table;
SET execution.checkpointing.interval = 1min;
SET parallelism.default = 3;
SET state.backend = rocksdb;
SET execution.checkpointing.timeout = 10min;


CREATE TABLE IF NOT EXISTS test_table(
`id` INT,
`class_id` INT,
`student_id` INT,
`course` STRING,
PRIMARY KEY(id) NOT ENFORCED
) COMMENT ''
WITH (
'connector' = 'mysql-cdc',
'hostname' = 'localhost',
'port' = '3306',
'username' = '<replace with your username>',
'password' = '<replace with your password>',
'database-name' = 'test',
'table-name' = 'test_table',
'server-time-zone'='Asia/Shanghai'
);
CREATE TABLE print_table (
id INT,
class_id INT,
student_id INT,
course STRING,
PRIMARY KEY(id) NOT ENFORCED
) WITH (
'connector' = 'print'
);
INSERT INTO print_table SELECT * FROM test_table;
```

### 4. run job

```bash
./bin/start-seatunnel-sql.sh --config <path/to/your/config>
```
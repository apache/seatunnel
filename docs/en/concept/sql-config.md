# SQL Configuration File

## Structure of SQL Configuration File

The `SQL` configuration file appears as follows.

### SQL

```sql
/* config
env {
  parallelism = 1
  job.mode = "BATCH"
}
*/

CREATE TABLE source_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'query' = 'select * from source',
  'properties'= '{
    useSSL = false,
    rewriteBatchedStatements = true
  }',
  'type'='source'
);

CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink',
  'type'='sink'
);

CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;

INSERT INTO sink_table SELECT * FROM temp1;
```

## Explanation of `SQL` Configuration File

### General Configuration in SQL File

```sql
/* config
env {
  parallelism = 1
  job.mode = "BATCH"
}
*/
```

In the `SQL` file, common configuration sections are defined using `/* config */` comments. Inside, common configurations like `env` can be defined using `HOCON` format.

### SOURCE SQL Syntax

```sql
CREATE TABLE source_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'query' = 'select * from source',
  'properties' = '{
    useSSL = false,
    rewriteBatchedStatements = true
  }',
  'type'='source'
);
```

* Using `CREATE TABLE ... WITH (...)` syntax creates a mapping for the source table. The `TABLE` name is the name of the source-mapped table, and the `WITH` syntax contains source-related configuration parameters.
* There are two fixed parameters in the WITH syntax: `connector` and `type`, representing connector type (such as `jdbc`, `FakeSource`, etc.) and source type (fixed as `source`), respectively.
* Other parameter names can reference relevant configuration parameters of the corresponding connector plugin, but the format needs to be changed to `'key' = 'value',`.
* If `'value'` is a sub-configuration, you can directly use a string in `HOCON` format. Note: if using a sub-configuration in `HOCON` format, the internal property items must be separated by `,`, like this:

```sql
'properties' = '{
  useSSL = false,
  rewriteBatchedStatements = true
}'
```

* If using `'` within `'value'`, it needs to be escaped with `''`, like this:

```sql
'query' = 'select * from source where name = ''Joy Ding'''
```

### SINK SQL Syntax

```sql
CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink',
  'type'='sink'
);
```

* Using `CREATE TABLE ... WITH (...)` syntax creates a mapping for the target table. The `TABLE` name is the name of the target-mapped table, and the `WITH` syntax contains sink-related configuration parameters.
* There are two fixed parameters in the `WITH` syntax: `connector` and `type`, representing connector type (such as `jdbc`, `console`, etc.) and target type (fixed as `sink`), respectively.
* Other parameter names can reference relevant configuration parameters of the corresponding connector plugin, but the format needs to be changed to `'key' = 'value',`.
* Optional configuration: `source_table_name`, which specifies the source table name, like this:

```sql
CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink',
  'source_table_name' = 'source_table',
  'type'='sink'
);

```

* If `source_table_name` is not specified, then `INSERT INTO ... SELECT ...` syntax is used to achieve the corresponding functionality, refer to: `INSERT SQL Syntax`

### INSERT SQL Syntax

```sql
INSERT INTO sink_table SELECT id, name, age, email FROM source_table;
```

* The `SELECT FROM` part is the table name of the source-mapped table.
* The `INSERT INTO` part is the table name of the target-mapped table.
* Note: This syntax does **not support** specifying fields in `INSERT`, like this: `INSERT INTO sink_table (id, name, age, email) SELECT id, name, age, email FROM source_table;`

### CREATE TABLE AS Syntax

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;
```

* This syntax creates a temporary table with the result of a `SELECT` query, used for subsequent `INSERT INTO` operations or `source_table_name` of `SINK TABLE`.
* The syntax of the `SELECT` part refers to: [SQL-transform](../transform-v2/sql.md) `query` configuration item

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;

INSERT INTO sink_table SELECT * FROM temp1;
```

or

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;

CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink',
  'source_table_name' = 'temp1',
  'type'='sink'
);
```

## Example of SQL Configuration File Submission

```bash
./bin/seatunnel.sh --config ./config/sample.sql
```


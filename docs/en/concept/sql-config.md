# SQL Configuration File

Before writing the sql config file, please make sure that the name of the config file should end with `.sql`.

## Structure of SQL Configuration File

The `SQL` configuration file appears as follows:

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
  'type'='source',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'query' = 'select * from source',
  'properties'= '{
    useSSL = false,
    rewriteBatchedStatements = true
  }'
);

CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'type'='sink',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink'
);

INSERT INTO sink_table SELECT id, name, age, email FROM source_table;
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
  'type'='source',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'query' = 'select * from source',
  'properties' = '{
    useSSL = false,
    rewriteBatchedStatements = true
  }'
);
```

* Using `CREATE TABLE ... WITH (...)` syntax creates a mapping for the source table. The `TABLE` name is the name of the source-mapped table, and the `WITH` syntax contains source-related configuration parameters.
* There are two fixed parameters in the WITH syntax: `connector` and `type`, representing connector plugin name (such as `jdbc`, `FakeSource`, etc.) and source type (fixed as `source`), respectively.
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
  'type'='sink',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink'
);
```

* Using `CREATE TABLE ... WITH (...)` syntax creates a mapping for the target table. The `TABLE` name is the name of the target-mapped table, and the `WITH` syntax contains sink-related configuration parameters.
* There are two fixed parameters in the `WITH` syntax: `connector` and `type`, representing connector plugin name (such as `jdbc`, `console`, etc.) and target type (fixed as `sink`), respectively.
* Other parameter names can reference relevant configuration parameters of the corresponding connector plugin, but the format needs to be changed to `'key' = 'value',`.

### INSERT INTO SELECT Syntax

```sql
INSERT INTO sink_table SELECT id, name, age, email FROM source_table;
```

* The `SELECT FROM` part is the table name of the source-mapped table. If the select field has keyword([refrence](https://github.com/JSQLParser/JSqlParser/blob/master/src/main/jjtree/net/sf/jsqlparser/parser/JSqlParserCC.jjt)),you should use it like \`filedName\`.
```sql
INSERT INTO sink_table SELECT id, name, age, email,`output` FROM source_table;
```
* The `INSERT INTO` part is the table name of the target-mapped table.
* Note: This syntax does **not support** specifying fields in `INSERT`, like this: `INSERT INTO sink_table (id, name, age, email) SELECT id, name, age, email FROM source_table;`

### INSERT INTO SELECT TABLE Syntax

```sql
INSERT INTO sink_table SELECT source_table;
```

* The `SELECT` part directly uses the name of the source-mapped table, indicating that all data from the source table will be inserted into the target table.
* Using this syntax does not generate related `transform` configurations. This syntax is generally used in multi-table synchronization scenarios. For example:

```sql
CREATE TABLE source_table WITH (
  'connector'='jdbc',
  'type' = 'source',
  'url' = 'jdbc:mysql://127.0.0.1:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'table_list' = '[
      {
        table_path = "source.table1"
      },
      {
        table_path = "source.table2",
        query = "select * from source.table2"
      }
    ]'
);

CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'type' = 'sink',
  'url' = 'jdbc:mysql://127.0.0.1:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'sink'
);

INSERT INTO sink_table SELECT source_table;
```

### CREATE TABLE AS Syntax

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;
```

* This syntax creates a temporary table with the result of a `SELECT` query, used for `INSERT INTO` operations.
* The syntax of the `SELECT` part refers to: [SQL Transform](../transform-v2/sql.md) `query` configuration item

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;

INSERT INTO sink_table SELECT * FROM temp1;
```

## Example of SQL Configuration File Submission

```bash
./bin/seatunnel.sh --config ./config/sample.sql
```


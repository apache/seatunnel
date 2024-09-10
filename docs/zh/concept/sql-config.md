# SQL配置文件

在编写`SQL`配置文件之前，请确保配置文件的名称应该以`.sql`结尾。

## SQL配置文件结构

`SQL`配置文件类似下面这样：

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

## `SQL`配置文件说明

### 通用配置

```sql
/* config
env {
  parallelism = 1
  job.mode = "BATCH"
}
*/
```

在`SQL`文件中通过 `/* config */` 注释定义通用配置部分，内部可以使用`hocon`格式定义通用的配置，如`env`等。

### SOURCE SQL语法

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

* 使用 `CREATE TABLE ... WITH (...)` 语法可创建源端表映射, `TABLE`表名为源端映射的表名，`WITH`语法中为源端相关的配置参数
* 在WITH语法中有两个固定参数：`connector` 和 `type`，分别表示连接器插件名（如：`jdbc`、`FakeSource`等）和源端类型（固定为：`source`）
* 其它参数名可以参考对应连接器插件的相关配置参数，但是格式需要改为`'key' = 'value',`的形式
* 如果`'value'`为一个子配置，可以直接使用`hocon`格式的字符串，注意：如果使用`hocon`格式的子配置，内部的属性项之间必须用`,`分隔！如：

```sql
'properties' = '{
  useSSL = false,
  rewriteBatchedStatements = true
}'
```

* 如果在`'value'`中使用到`'`，需要用`''`进行转义，如：

```sql
'query' = 'select * from source where name = ''Joy Ding'''
```

### SINK SQL语法

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

* 使用 `CREATE TABLE ... WITH (...)` 语法可创建目标端表映射, `TABLE`表名为目标端映射的表名，`WITH`语法中为目标端相关的配置参数
* 在WITH语法中有两个固定参数：`connector` 和 `type`，分别表示连接器插件名（如：`jdbc`、`console`等）和目标端类型（固定为：`sink`）
* 其它参数名可以参考对应连接器插件的相关配置参数，但是格式需要改为`'key' = 'value',`的形式

### INSERT INTO SELECT语法

```sql
INSERT INTO sink_table SELECT id, name, age, email FROM source_table;
```

* `SELECT FROM` 部分为源端映射表的表名，`SELECT` 部分的语法参考：[SQL-transform](../transform-v2/sql.md) `query` 配置项。如果select的字段是关键字([参考](https://github.com/JSQLParser/JSqlParser/blob/master/src/main/jjtree/net/sf/jsqlparser/parser/JSqlParserCC.jjt))，你应该像这样使用\`filedName\`
```sql
INSERT INTO sink_table SELECT id, name, age, email,`output` FROM source_table;
```
* `INSERT INTO` 部分为目标端映射表的表名
* 注意：该语法**不支持**在 `INSERT` 中指定字段，如：`INSERT INTO sink_table (id, name, age, email) SELECT id, name, age, email FROM source_table;`

### INSERT INTO SELECT TABLE语法

```sql
INSERT INTO sink_table SELECT source_table;
```

* `SELECT` 部分直接使用源端映射表的表名，表示将源端表的所有数据插入到目标端表中
* 使用该语法不会生成`trasform`的相关配置，这种语法一般用在多表同步的场景，示例：

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

### CREATE TABLE AS语法

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;
```

* 该语法可以将一个`SELECT`查询结果作为一个临时表，用于的`INSERT INTO`操作
* `SELECT` 部分的语法参考：[SQL Transform](../transform-v2/sql.md) `query` 配置项

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;

INSERT INTO sink_table SELECT * FROM temp1;
```

## SQL配置文件任务提交示例

```bash
./bin/seatunnel.sh --config ./config/sample.sql
```


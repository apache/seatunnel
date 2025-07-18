# DataX JDBC 数据源配置样例说明

## 概述

本文档说明了四个典型的DataX JDBC数据源配置样例，涵盖了MySQL、PostgreSQL、Oracle、SQL Server四种主流数据库，统一以HDFS作为目标存储。这些配置样例旨在验证X2SeaTunnel工具的JDBC源模板能否正确进行参数映射和配置转换。

## 配置样例详情

### 1. MySQL 数据源 (datax-mysql2hdfs-full.json)

**数据库特点：**
- 使用MySQL 8.0+ 推荐的驱动：`com.mysql.cj.jdbc.Driver`
- 连接URL包含SSL和时区设置
- 支持分片并行读取（splitPk）

**配置要点：**
```json
{
  "jdbcUrl": "jdbc:mysql://localhost:3306/test_db?useSSL=false&serverTimezone=UTC",
  "username": "root",
  "password": "password",
  "splitPk": "id",
  "fetchSize": 1000,
  "where": "age > 18"
}
```

**SeaTunnel映射：**
- `url`: 直接映射连接URL
- `driver`: 自动推断为MySQL驱动
- `user/password`: 直接映射认证信息
- `partition_column`: 映射splitPk用于并行读取
- `query`: 根据column、table、where自动生成SELECT语句

### 2. PostgreSQL 数据源 (datax-postgresql2hdfs-full.json)

**数据库特点：**
- 使用PostgreSQL官方驱动：`org.postgresql.Driver`
- 支持预编译语句缓存优化
- 强类型系统，适合复杂数据类型

**配置要点：**
```json
{
  "jdbcUrl": "jdbc:postgresql://localhost:5432/ecommerce?useSSL=false",
  "username": "postgres", 
  "password": "password",
  "fetchSize": 2000,
  "splitPk": "id"
}
```

**SeaTunnel映射：**
- PostgreSQL特有的连接参数通过properties传递
- 支持更大的fetchSize（2000）提高读取效率
- 输出格式为CSV，压缩格式为gzip

### 3. Oracle 数据源 (datax-oracle2hdfs-full.json)

**数据库特点：**
- 使用Oracle官方驱动：`oracle.jdbc.driver.OracleDriver`
- 表名和列名通常为大写
- 支持复杂的企业级特性

**配置要点：**
```json
{
  "jdbcUrl": "jdbc:oracle:thin:@localhost:1521:orcl",
  "username": "scott",
  "password": "tiger",
  "fetchSize": 500,
  "splitPk": "EMP_ID"
}
```

**SeaTunnel映射：**
- Oracle特有的日期处理参数
- 较小的fetchSize（500）适应Oracle的内存管理
- 支持大写的表名和列名

### 4. SQL Server 数据源 (datax-sqlserver2hdfs-full.json)

**数据库特点：**
- 使用Microsoft官方驱动：`com.microsoft.sqlserver.jdbc.SQLServerDriver`
- 连接URL包含加密设置
- 支持Windows身份验证

**配置要点：**
```json
{
  "jdbcUrl": "jdbc:sqlserver://localhost:1433;DatabaseName=SalesDB;encrypt=false",
  "username": "sa",
  "password": "Password123",
  "fetchSize": 1500,
  "splitPk": "OrderID"
}
```

**SeaTunnel映射：**
- SQL Server特有的连接参数和加密设置
- 适中的fetchSize（1500）平衡性能和内存使用
- 输出使用Snappy压缩提高效率

## 统一的HDFS Sink配置

所有配置样例都使用相同的HDFS sink结构：

```json
{
  "name": "hdfswriter",
  "parameter": {
    "defaultFS": "hdfs://localhost:9000",
    "fileType": "text",
    "path": "/user/seatunnel/output/{database}_data",
    "fileName": "{table_name}",
    "writeMode": "append/overwrite",
    "fieldDelimiter": "\t/,/|",
    "compress": "none/gzip/snappy",
    "encoding": "UTF-8"
  }
}
```

## 参数映射验证要点

### 必选参数映射
1. **url**: `${datax:job.content[0].reader.parameter.connection[0].jdbcUrl[0]}`
2. **driver**: `${datax:job.content[0].reader.parameter.connection[0].jdbcUrl[0]|@jdbc_driver_mapper}`
3. **user**: `${datax:job.content[0].reader.parameter.username}`
4. **password**: `${datax:job.content[0].reader.parameter.password}`
5. **query**: 根据column、table、where自动生成或使用querySql

### 可选参数映射
1. **partition_column**: `${datax:job.content[0].reader.parameter.splitPk}`
2. **partition_num**: `${datax:job.setting.speed.channel}`
3. **fetch_size**: `${datax:job.content[0].reader.parameter.fetchSize}`

### 转换器验证
- `@jdbc_driver_mapper`: 根据jdbcUrl自动推断驱动类名
- 支持MySQL、PostgreSQL、Oracle、SQL Server的驱动映射

## 使用方法

1. **编译X2SeaTunnel工具**：
   ```bash
   cd seatunnel-tools/x2seatunnel
   mvn clean package -DskipTests
   ```

2. **执行转换测试**：
   ```bash
   chmod +x test-jdbc-conversion.sh
   ./test-jdbc-conversion.sh
   ```

3. **验证转换结果**：
   检查生成的SeaTunnel配置文件，确保：
   - 所有必选参数正确映射
   - 驱动类名正确推断
   - 查询语句正确生成
   - 可选参数合理设置

## 预期输出

转换成功后，每个DataX配置都会生成对应的SeaTunnel配置文件：
- `datax-mysql2hdfs-full_seatunnel.conf`
- `datax-postgresql2hdfs-full_seatunnel.conf`
- `datax-oracle2hdfs-full_seatunnel.conf`
- `datax-sqlserver2hdfs-full_seatunnel.conf`

这些配置文件应包含完整的JDBC Source配置，可直接在SeaTunnel中使用。

## 注意事项

1. **驱动依赖**: 确保运行时环境包含对应的JDBC驱动JAR包
2. **网络连接**: 确保SeaTunnel能够访问目标数据库
3. **权限配置**: 确保数据库用户具有相应的读取权限
4. **性能调优**: 根据实际数据量调整partition_num和fetch_size参数
5. **类型映射**: 注意不同数据库的数据类型差异，必要时启用类型窄化

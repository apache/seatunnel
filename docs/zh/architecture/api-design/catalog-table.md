---
sidebar_position: 4
title: CatalogTable 和元数据管理
---

# CatalogTable 和元数据管理

## 1. 概述

### 1.1 问题背景

数据集成需要显式的模式管理:

- **模式定义**: 如何定义和验证表模式?
- **模式传播**: 如何在数据源 → 转换器 → 目标端之间传递模式?
- **模式演化**: 如何处理运行时 DDL 变更(添加/删除列)?
- **类型映射**: 如何在不同数据源之间映射类型?
- **元数据完整性**: 如何捕获完整的表元数据(约束、分区)?

### 1.2 设计目标

SeaTunnel 的元数据管理旨在:

1. **类型安全**: 在作业提交时进行显式模式验证
2. **完整性**: 捕获所有表元数据(列、约束、分区、选项)
3. **支持演化**: 处理运行时模式变更(DDL 同步)
4. **引擎独立**: 模式表示独立于执行引擎
5. **易用性**: 用于模式创建和转换的简单 API

## 2. 核心概念

### 2.1 CatalogTable

包含所有元数据的表的完整表示。

```java
public class CatalogTable implements Serializable {
    // 表标识符
    private final TableIdentifier tableId;

    // 模式定义
    private final TableSchema tableSchema;

    // 表选项(连接器特定配置)
    private final Map<String, String> options;

    // 分区键
    private final List<String> partitionKeys;

    // 注释
    private final String comment;

    // 目录名称
    private final String catalogName;
}
```

**关键组件**:
- `TableIdentifier`: 唯一表标识(catalog.database.table)
- `TableSchema`: 包含列、主键、约束的模式
- `options`: 连接器特定设置(例如 Kafka 主题、JDBC 表名)
- `partitionKeys`: 分区表的分区列

### 2.2 TableSchema

包含列和约束的模式定义。

```java
public class TableSchema implements Serializable {
    // 列定义
    private final List<Column> columns;

    // 主键
    private final PrimaryKey primaryKey;

    // 唯一键/外键约束
    private final List<ConstraintKey> constraintKeys;
}
```

### 2.3 Column

包含类型和约束的列定义。

```java
public class Column implements Serializable {
    private final String name;
    private final SeaTunnelDataType<?> dataType;
    private final String comment;

    // 列选项
    private final Map<String, Object> options;

    // 约束
    private final boolean nullable;
    private final Object defaultValue;
}
```

### 2.4 SeaTunnelDataType

跨连接器的统一类型系统。

**基本类型**:
```java
// 数值
DataTypes.TINYINT()
DataTypes.SMALLINT()
DataTypes.INT()
DataTypes.BIGINT()
DataTypes.FLOAT()
DataTypes.DOUBLE()
DataTypes.DECIMAL(precision, scale)

// 字符串
DataTypes.STRING()
DataTypes.CHAR(length)
DataTypes.VARCHAR(length)

// 二进制
DataTypes.BYTES()

// 日期/时间
DataTypes.DATE()
DataTypes.TIME()
DataTypes.TIMESTAMP()

// 布尔
DataTypes.BOOLEAN()
```

**复杂类型**:
```java
// 数组
DataTypes.ARRAY(elementType)

// 映射
DataTypes.MAP(keyType, valueType)

// 行(结构体)
DataTypes.ROW(fields)
```

## 3. 模式创建

### 3.1 构建器模式

```java
CatalogTable catalogTable = CatalogTable.of(
    TableIdentifier.of("my_catalog", "my_db", "my_table"),
    TableSchema.builder()
        .column("id", DataTypes.BIGINT())
        .column("name", DataTypes.STRING())
        .column("age", DataTypes.INT())
        .column("created_at", DataTypes.TIMESTAMP())
        .primaryKey("id")
        .build(),
    Map.of("connector", "jdbc"),
    Collections.emptyList(), // 无分区
    "User table"
);
```

### 3.2 列构建器

```java
Column column = Column.builder()
    .name("user_id")
    .dataType(DataTypes.BIGINT())
    .nullable(false)
    .defaultValue(0L)
    .comment("User identifier")
    .build();
```

### 3.3 主键和约束

```java
TableSchema schema = TableSchema.builder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING())
    .column("username", DataTypes.STRING())

    // 主键
    .primaryKey("id")

    // 唯一约束
    .constraint(ConstraintKey.of(
        ConstraintKey.ConstraintType.UNIQUE_KEY,
        "uk_email",
        Arrays.asList(
            ConstraintKey.ConstraintKeyColumn.of("email", null)
        )
    ))

    .build();
```

## 4. 模式传播

### 4.1 数据源 → 转换器 → 目标端流程

```
┌──────────────┐
│    数据源     │
│              │
│  生产        │
│ CatalogTable │
└──────┬───────┘
       │
       ▼ (输入模式)
┌──────────────┐
│   转换器     │
│              │
│  修改        │
│ CatalogTable │
└──────┬───────┘
       │
       ▼ (输出模式)
┌──────────────┐
│   目标端     │
│              │
│  验证        │
│ CatalogTable │
└──────────────┘
```

### 4.2 数据源模式生产

```java
public class JdbcSource implements SeaTunnelSource<...> {
    @Override
    public CatalogTable getProducedCatalogTable() {
        // 从数据库元数据读取模式
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, schema, table, null);

        // 构建模式
        TableSchema.Builder builder = TableSchema.builder();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            int jdbcType = columns.getInt("DATA_TYPE");
            SeaTunnelDataType<?> type = JdbcTypeConverter.convert(jdbcType);

            builder.column(columnName, type);
        }

        return CatalogTable.of(
            TableIdentifier.of(catalog, schema, table),
            builder.build()
        );
    }
}
```

### 4.3 转换器模式转换

```java
public class SqlTransform implements SeaTunnelTransform {
    @Override
    public CatalogTable getProducedCatalogTable() {
        CatalogTable inputTable = getInputCatalogTable();

        // 解析 SQL 推断输出模式
        // 示例: SELECT id, UPPER(name) as name_upper, age FROM input
        TableSchema outputSchema = TableSchema.builder()
            .column("id", inputTable.getColumn("id").getDataType())
            .column("name_upper", DataTypes.STRING()) // 已转换
            .column("age", inputTable.getColumn("age").getDataType())
            .build();

        return inputTable.copy(outputSchema);
    }
}
```

### 4.4 目标端模式验证

```java
public class JdbcSink implements SeaTunnelSink<...> {
    @Override
    public CatalogTable getWriteCatalogTable() {
        // 验证输入模式与目标表匹配
        CatalogTable inputTable = getInputCatalogTable();
        CatalogTable targetTable = readTargetTableSchema();

        // 检查列兼容性
        for (Column inputColumn : inputTable.getColumns()) {
            Column targetColumn = targetTable.getColumn(inputColumn.getName());
            if (targetColumn == null) {
                throw new SchemaException("Column not found: " + inputColumn.getName());
            }

            if (!isCompatible(inputColumn.getDataType(), targetColumn.getDataType())) {
                throw new SchemaException("Incompatible types for " + inputColumn.getName());
            }
        }

        return targetTable;
    }
}
```

## 5. 模式演化

### 5.1 SchemaChangeEvent

表示 CDC 数据源捕获的 DDL 变更。

```java
public abstract class SchemaChangeEvent implements Serializable {
    private final TableIdentifier tableId;
}

public class AlterTableAddColumnEvent extends SchemaChangeEvent {
    private final Column column;
}

public class AlterTableDropColumnEvent extends SchemaChangeEvent {
    private final String columnName;
}

public class AlterTableModifyColumnEvent extends SchemaChangeEvent {
    private final Column column;
}
```

### 5.2 CDC 数据源模式演化

```java
public class MysqlCDCSource {
    private void handleDDL(String ddl) {
        // 解析 DDL 语句
        if (ddl.contains("ADD COLUMN")) {
            Column newColumn = parseDDL(ddl);

            // 创建模式变更事件
            SchemaChangeEvent event = new AlterTableAddColumnEvent(
                tableId,
                newColumn
            );

            // 向下游发送事件
            collector.collect(event);
        }
    }
}
```

### 5.3 转换器模式演化映射

```java
public class SqlTransform {
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent addEvent = (AlterTableAddColumnEvent) event;

            // 通过转换逻辑映射列
            Column transformedColumn = transformColumn(addEvent.getColumn());

            return new AlterTableAddColumnEvent(
                event.getTableId(),
                transformedColumn
            );
        }

        return event; // 传递
    }
}
```

### 5.4 目标端模式演化应用

```java
public class JdbcSink {
    private void applySchemaChange(SchemaChangeEvent event) {
        if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent addEvent = (AlterTableAddColumnEvent) event;
            Column column = addEvent.getColumn();

            // 生成 DDL
            String ddl = String.format(
                "ALTER TABLE %s ADD COLUMN %s %s",
                event.getTableId().getTableName(),
                column.getName(),
                toSqlType(column.getDataType())
            );

            // 执行 DDL
            statement.execute(ddl);

            LOG.info("Applied schema change: {}", ddl);
        }
    }
}
```

## 6. 类型映射

### 6.1 JDBC 类型映射

```java
public class JdbcTypeConverter {
    public static SeaTunnelDataType<?> convert(int jdbcType) {
        switch (jdbcType) {
            case Types.TINYINT:
                return DataTypes.TINYINT();
            case Types.SMALLINT:
                return DataTypes.SMALLINT();
            case Types.INTEGER:
                return DataTypes.INT();
            case Types.BIGINT:
                return DataTypes.BIGINT();
            case Types.FLOAT:
            case Types.REAL:
                return DataTypes.FLOAT();
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
            case Types.DECIMAL:
            case Types.NUMERIC:
                return DataTypes.DECIMAL(precision, scale);
            case Types.CHAR:
                return DataTypes.CHAR(length);
            case Types.VARCHAR:
                return DataTypes.VARCHAR(length);
            case Types.LONGVARCHAR:
                return DataTypes.STRING();
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIME:
                return DataTypes.TIME();
            case Types.TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case Types.BOOLEAN:
                return DataTypes.BOOLEAN();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedTypeException("Unsupported JDBC type: " + jdbcType);
        }
    }
}
```

### 6.2 Kafka (Avro) 类型映射

```java
public class AvroTypeConverter {
    public static SeaTunnelDataType<?> convert(Schema avroSchema) {
        switch (avroSchema.getType()) {
            case INT:
                return DataTypes.INT();
            case LONG:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case STRING:
                return DataTypes.STRING();
            case BYTES:
                return DataTypes.BYTES();
            case ARRAY:
                return DataTypes.ARRAY(convert(avroSchema.getElementType()));
            case MAP:
                return DataTypes.MAP(
                    DataTypes.STRING(),
                    convert(avroSchema.getValueType())
                );
            case RECORD:
                // 转换为 ROW 类型
                List<TableSchema.Column> fields = new ArrayList<>();
                for (Schema.Field field : avroSchema.getFields()) {
                    fields.add(new Column(
                        field.name(),
                        convert(field.schema())
                    ));
                }
                return DataTypes.ROW(fields);
            default:
                throw new UnsupportedTypeException("Unsupported Avro type: " + avroSchema.getType());
        }
    }
}
```

## 7. 分区表

### 7.1 分区定义

```java
CatalogTable catalogTable = CatalogTable.of(
    tableId,
    schema,
    options,
    Arrays.asList("year", "month", "day"), // 分区键
    comment
);
```

### 7.2 分区感知数据源

```java
public class HiveSource {
    @Override
    public CatalogTable getProducedCatalogTable() {
        // 读取 Hive 表元数据
        Table hiveTable = hiveMetastore.getTable(dbName, tableName);

        // 提取分区键
        List<String> partitionKeys = hiveTable.getPartitionKeys().stream()
            .map(FieldSchema::getName)
            .collect(Collectors.toList());

        return CatalogTable.of(
            tableId,
            schema,
            options,
            partitionKeys,
            comment
        );
    }
}
```

### 7.3 分区感知目标端

```java
public class IcebergSink {
    private void write(SeaTunnelRow row, CatalogTable table) {
        // 从行中提取分区值
        Map<String, Object> partitionValues = new HashMap<>();
        for (String partitionKey : table.getPartitionKeys()) {
            int index = table.getSchema().indexOf(partitionKey);
            partitionValues.put(partitionKey, row.getField(index));
        }

        // 写入正确的分区
        PartitionSpec spec = PartitionSpec.builderFor(schema)
            .identity("year")
            .identity("month")
            .identity("day")
            .build();

        DataFile dataFile = writeToPartition(partitionValues, row);
        icebergTable.newAppend().appendFile(dataFile).commit();
    }
}
```

## 8. 最佳实践

### 8.1 模式定义

**优先使用显式模式**:
```java
// ✅ 好: 显式模式
TableSchema schema = TableSchema.builder()
    .column("id", DataTypes.BIGINT())
    .column("name", DataTypes.STRING())
    .build();

// ❌ 差: 隐式模式(依赖推断)
// 从第一行推断模式 - 有风险!
```

**使用适当的类型**:
```java
// ✅ 好: 使用特定类型
.column("price", DataTypes.DECIMAL(10, 2))
.column("created_at", DataTypes.TIMESTAMP())

// ❌ 差: 过于通用的类型
.column("price", DataTypes.STRING()) // 应该是 DECIMAL
.column("created_at", DataTypes.STRING()) // 应该是 TIMESTAMP
```

### 8.2 模式验证

**早期验证**:
```java
// 在数据源中
@Override
public void open() {
    CatalogTable catalogTable = getProducedCatalogTable();
    validateSchema(catalogTable); // 快速失败
}

// 在目标端中
@Override
public void open() {
    CatalogTable inputTable = getInputCatalogTable();
    CatalogTable targetTable = getWriteCatalogTable();
    validateCompatibility(inputTable, targetTable); // 快速失败
}
```

### 8.3 类型兼容性

**类型扩展(安全)**:
```java
// INT → BIGINT (安全)
// FLOAT → DOUBLE (安全)
// VARCHAR(10) → VARCHAR(20) (安全)
```

**类型缩小(不安全)**:
```java
// BIGINT → INT (可能溢出)
// DOUBLE → FLOAT (精度损失)
// VARCHAR(20) → VARCHAR(10) (截断)
```

## 9. 配置

### 9.1 模式覆盖

```hocon
source {
  JDBC {
    url = "..."
    query = "SELECT * FROM users"

    # 覆盖推断的模式
    schema {
      fields {
        id = "BIGINT"
        name = "STRING"
        age = "INT"
      }
    }
  }
}
```

### 9.2 模式演化控制

```hocon
sink {
  JDBC {
    url = "..."

    # 模式演化选项
    schema-evolution {
      enabled = true
      auto-create-table = true
      auto-add-column = true
      auto-drop-column = false # 危险!
    }
  }
}
```

## 10. 相关资源

- [数据源架构](source-architecture.md)
- [目标端架构](sink-architecture.md)
- [模式演化](../../introduction/concepts/schema-evolution.md)
- [模式特性](../../introduction/concepts/schema-feature.md)

## 11. 参考资料

### 关键源文件

- [CatalogTable.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/catalog/CatalogTable.java)
- [TableSchema.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/catalog/TableSchema.java)
- [Column.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/catalog/Column.java)
- [SeaTunnelDataType.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/type/SeaTunnelDataType.java)
- [SchemaChangeEvent.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/event/SchemaChangeEvent.java)

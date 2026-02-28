---
sidebar_position: 4
title: CatalogTable and Metadata Management
---

# CatalogTable and Metadata Management

## 1. Overview

### 1.1 Problem Background

Data integration requires explicit schema management:

- **Schema Definition**: How to define and validate table schemas?
- **Schema Propagation**: How to pass schema through Source → Transform → Sink?
- **Schema Evolution**: How to handle runtime DDL changes (ADD/DROP columns)?
- **Type Mapping**: How to map types between different data sources?
- **Metadata Completeness**: How to capture complete table metadata (constraints, partitions)?

### 1.2 Design Goals

SeaTunnel's metadata management aims to:

1. **Type Safety**: Explicit schema validation at job submission
2. **Completeness**: Capture all table metadata (columns, constraints, partitions, options)
3. **Evolution Support**: Handle runtime schema changes (DDL synchronization)
4. **Engine Independence**: Schema representation independent of execution engine
5. **Ease of Use**: Simple API for schema creation and transformation

## 2. Core Concepts

### 2.1 CatalogTable

Complete representation of a table with all metadata.

```java
public class CatalogTable implements Serializable {
    // Table identifier
    private final TableIdentifier tableId;

    // Schema definition
    private final TableSchema tableSchema;

    // Table options (connector-specific configuration)
    private final Map<String, String> options;

    // Partition keys
    private final List<String> partitionKeys;

    // Comment
    private final String comment;

    // Catalog name
    private final String catalogName;
}
```

**Key Components**:
- `TableIdentifier`: Unique table identity (`catalog.database[.schema].table`)
- `TableSchema`: Schema with columns, primary key, constraints
- `options`: Connector-specific settings (e.g., Kafka topic, JDBC table name)
- `partitionKeys`: Partition columns for partitioned tables

### 2.2 TableSchema

Schema definition with columns and constraints.

```java
public class TableSchema implements Serializable {
    // Column definitions
    private final List<Column> columns;

    // Primary key
    private final PrimaryKey primaryKey;

    // Unique/foreign key constraints
    private final List<ConstraintKey> constraintKeys;
}
```

### 2.3 Column

Column definition with type and constraints.

```java
public class Column implements Serializable {
    private final String name;
    private final SeaTunnelDataType<?> dataType;
    private final String comment;

    // Column options
    private final Map<String, Object> options;

    // Constraints
    private final boolean nullable;
    private final Object defaultValue;
}
```

### 2.4 SeaTunnelDataType

Unified type system across connectors.

**Basic Types**:
```java
// Numeric
DataTypes.TINYINT()
DataTypes.SMALLINT()
DataTypes.INT()
DataTypes.BIGINT()
DataTypes.FLOAT()
DataTypes.DOUBLE()
DataTypes.DECIMAL(precision, scale)

// String
DataTypes.STRING()
DataTypes.CHAR(length)
DataTypes.VARCHAR(length)

// Binary
DataTypes.BYTES()

// Date/Time
DataTypes.DATE()
DataTypes.TIME()
DataTypes.TIMESTAMP()

// Boolean
DataTypes.BOOLEAN()
```

**Complex Types**:
```java
// Array
DataTypes.ARRAY(elementType)

// Map
DataTypes.MAP(keyType, valueType)

// Row (Struct)
DataTypes.ROW(fields)
```

## 3. Schema Creation

### 3.1 Builder Pattern

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
    Collections.emptyList(), // No partitions
    "User table"
);
```

### 3.2 Column Builder

```java
Column column = Column.builder()
    .name("user_id")
    .dataType(DataTypes.BIGINT())
    .nullable(false)
    .defaultValue(0L)
    .comment("User identifier")
    .build();
```

### 3.3 Primary Key and Constraints

```java
TableSchema schema = TableSchema.builder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING())
    .column("username", DataTypes.STRING())

    // Primary key
    .primaryKey("id")

    // Unique constraint
    .constraint(ConstraintKey.of(
        ConstraintKey.ConstraintType.UNIQUE_KEY,
        "uk_email",
        Arrays.asList(
            ConstraintKey.ConstraintKeyColumn.of("email", null)
        )
    ))

    .build();
```

## 4. Schema Propagation

### 4.1 Source → Transform → Sink Flow

```
┌──────────────┐
│    Source    │
│              │
│  produces    │
│ CatalogTable │
└──────┬───────┘
       │
       ▼ (Input Schema)
┌──────────────┐
│  Transform   │
│              │
│  modifies    │
│ CatalogTable │
└──────┬───────┘
       │
       ▼ (Output Schema)
┌──────────────┐
│     Sink     │
│              │
│  validates   │
│ CatalogTable │
└──────────────┘
```

### 4.2 Source Schema Production

```java
public class JdbcSource implements SeaTunnelSource<...> {
    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        // Read schema from database metadata
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, schema, table, null);
        String database = "...";

        // Build schema
        TableSchema.Builder builder = TableSchema.builder();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            int jdbcType = columns.getInt("DATA_TYPE");
            SeaTunnelDataType<?> type = JdbcTypeConverter.convert(jdbcType);

            builder.column(columnName, type);
        }

        return Collections.singletonList(
            CatalogTable.of(
                TableIdentifier.of(catalog, database, schema, table),
                builder.build()
            )
        );
    }
}
```

### 4.3 Transform Schema Transformation

```java
public class SqlTransform implements SeaTunnelTransform {
    @Override
    public CatalogTable getProducedCatalogTable() {
        CatalogTable inputTable = getInputCatalogTable();

        // Parse SQL to infer output schema
        // Example: SELECT id, UPPER(name) as name_upper, age FROM input
        TableSchema outputSchema = TableSchema.builder()
            .column("id", inputTable.getColumn("id").getDataType())
            .column("name_upper", DataTypes.STRING()) // Transformed
            .column("age", inputTable.getColumn("age").getDataType())
            .build();

        return inputTable.copy(outputSchema);
    }
}
```

### 4.4 Sink Schema Validation

```java
public class JdbcSink implements SeaTunnelSink<...> {
    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        // Validate input schema matches target table
        CatalogTable inputTable = getInputCatalogTable();
        CatalogTable targetTable = readTargetTableSchema();

        // Check column compatibility
        for (Column inputColumn : inputTable.getColumns()) {
            Column targetColumn = targetTable.getColumn(inputColumn.getName());
            if (targetColumn == null) {
                throw new SchemaException("Column not found: " + inputColumn.getName());
            }

            if (!isCompatible(inputColumn.getDataType(), targetColumn.getDataType())) {
                throw new SchemaException("Incompatible types for " + inputColumn.getName());
            }
        }

        return Optional.of(targetTable);
    }
}
```

## 5. Schema Evolution

### 5.1 SchemaChangeEvent

Represents DDL changes captured by CDC sources.

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

### 5.2 CDC Source Schema Evolution

```java
public class MysqlCDCSource {
    private void handleDDL(String ddl) {
        // Parse DDL statement
        if (ddl.contains("ADD COLUMN")) {
            Column newColumn = parseDDL(ddl);

            // Create schema change event
            SchemaChangeEvent event = new AlterTableAddColumnEvent(
                tableId,
                newColumn
            );

            // Emit event downstream
            collector.collect(event);
        }
    }
}
```

### 5.3 Transform Schema Evolution Mapping

```java
public class SqlTransform {
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent addEvent = (AlterTableAddColumnEvent) event;

            // Map column through transform logic
            Column transformedColumn = transformColumn(addEvent.getColumn());

            return new AlterTableAddColumnEvent(
                event.getTableId(),
                transformedColumn
            );
        }

        return event; // Pass through
    }
}
```

### 5.4 Sink Schema Evolution Application

```java
public class JdbcSink {
    private void applySchemaChange(SchemaChangeEvent event) {
        if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent addEvent = (AlterTableAddColumnEvent) event;
            Column column = addEvent.getColumn();

            // Generate DDL
            String ddl = String.format(
                "ALTER TABLE %s ADD COLUMN %s %s",
                event.getTableId().getTableName(),
                column.getName(),
                toSqlType(column.getDataType())
            );

            // Execute DDL
            statement.execute(ddl);

            LOG.info("Applied schema change: {}", ddl);
        }
    }
}
```

## 6. Type Mapping

### 6.1 JDBC Type Mapping

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

### 6.2 Kafka (Avro) Type Mapping

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
                // Convert to ROW type
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

## 7. Partitioned Tables

### 7.1 Partition Definition

```java
CatalogTable catalogTable = CatalogTable.of(
    tableId,
    schema,
    options,
    Arrays.asList("year", "month", "day"), // Partition keys
    comment
);
```

### 7.2 Partition-Aware Source

```java
public class HiveSource {
    @Override
    public CatalogTable getProducedCatalogTable() {
        // Read Hive table metadata
        Table hiveTable = hiveMetastore.getTable(dbName, tableName);

        // Extract partition keys
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

### 7.3 Partition-Aware Sink

```java
public class IcebergSink {
    private void write(SeaTunnelRow row, CatalogTable table) {
        // Extract partition values from row
        Map<String, Object> partitionValues = new HashMap<>();
        for (String partitionKey : table.getPartitionKeys()) {
            int index = table.getSchema().indexOf(partitionKey);
            partitionValues.put(partitionKey, row.getField(index));
        }

        // Write to correct partition
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

## 8. Best Practices

### 8.1 Schema Definition

**Prefer Explicit Schema**:
```java
// ✅ GOOD: Explicit schema
TableSchema schema = TableSchema.builder()
    .column("id", DataTypes.BIGINT())
    .column("name", DataTypes.STRING())
    .build();

// ❌ BAD: Implicit schema (relies on inference)
// Schema inferred from first row - risky!
```

**Use Appropriate Types**:
```java
// ✅ GOOD: Use specific types
.column("price", DataTypes.DECIMAL(10, 2))
.column("created_at", DataTypes.TIMESTAMP())

// ❌ BAD: Overly generic types
.column("price", DataTypes.STRING()) // Should be DECIMAL
.column("created_at", DataTypes.STRING()) // Should be TIMESTAMP
```

### 8.2 Schema Validation

**Validate Early**:
```java
// In Source
@Override
public void open() {
    CatalogTable catalogTable = getProducedCatalogTables().get(0);
    validateSchema(catalogTable); // Fail fast
}

// In Sink
@Override
public void open() {
    CatalogTable inputTable = getInputCatalogTable();
    CatalogTable targetTable = getWriteCatalogTable().orElseThrow(IllegalStateException::new);
    validateCompatibility(inputTable, targetTable); // Fail fast
}
```

### 8.3 Type Compatibility

**Type Widening (Safe)**:
```java
// INT → BIGINT (safe)
// FLOAT → DOUBLE (safe)
// VARCHAR(10) → VARCHAR(20) (safe)
```

**Type Narrowing (Unsafe)**:
```java
// BIGINT → INT (may overflow)
// DOUBLE → FLOAT (precision loss)
// VARCHAR(20) → VARCHAR(10) (truncation)
```

## 9. Configuration

### 9.1 Schema Override

```hocon
source {
  JDBC {
    url = "..."
    query = "SELECT * FROM users"

    # Override inferred schema
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

### 9.2 Schema Evolution Control

```hocon
sink {
  JDBC {
    url = "..."

    # Schema evolution options
    schema-evolution {
      enabled = true
      auto-create-table = true
      auto-add-column = true
      auto-drop-column = false # Dangerous!
    }
  }
}
```

## 10. Related Resources

- [Source Architecture](source-architecture.md)
- [Sink Architecture](sink-architecture.md)
- [Schema Evolution](../../introduction/concepts/schema-evolution.md)
- [Schema Feature](../../introduction/concepts/schema-feature.md)

## 11. References

### Key Source Files

- [CatalogTable.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/catalog/CatalogTable.java)
- [TableSchema.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/catalog/TableSchema.java)
- [Column.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/catalog/Column.java)
- [SeaTunnelDataType.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/type/SeaTunnelDataType.java)
- [SchemaChangeEvent.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/event/SchemaChangeEvent.java)

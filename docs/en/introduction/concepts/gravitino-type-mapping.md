# Gravitino Type Mapping

This document describes the type mapping between Apache Gravitino and SeaTunnel when using Gravitino as the metadata source. The type conversion is handled by `GravitinoTableSchemaConvertor`.

## Overview

When SeaTunnel reads table schema from Gravitino, the Gravitino column types are automatically converted to corresponding SeaTunnel data types. This mapping enables seamless integration between Gravitino-managed metadata and SeaTunnel's data processing pipeline.

## Primitive Type Mapping

| Gravitino Type   | Gravitino JSON Representation | SeaTunnel Type                        | SeaTunnel Type Keyword | Java Type                  | Notes                                                     |
|:-----------------|:------------------------------|:--------------------------------------|:-----------------------|:---------------------------|:----------------------------------------------------------|
| Boolean          | `boolean`                     | `BasicType.BOOLEAN_TYPE`              | `boolean`              | `java.lang.Boolean`        | -                                                         |
| Byte             | `byte`                        | `BasicType.BYTE_TYPE`                 | `tinyint`              | `java.lang.Byte`           | -                                                         |
| Unsigned Byte    | `byte unsigned`               | `BasicType.BYTE_TYPE`                 | `tinyint`              | `java.lang.Byte`           | Unsigned flag is ignored                                  |
| Short            | `short`                       | `BasicType.SHORT_TYPE`                | `smallint`             | `java.lang.Short`          | -                                                         |
| Unsigned Short   | `short unsigned`              | `BasicType.SHORT_TYPE`                | `smallint`             | `java.lang.Short`          | Unsigned flag is ignored                                  |
| Integer          | `integer`                     | `BasicType.INT_TYPE`                  | `int`                  | `java.lang.Integer`        | -                                                         |
| Unsigned Integer | `integer unsigned`            | `BasicType.INT_TYPE`                  | `int`                  | `java.lang.Integer`        | Unsigned flag is ignored                                  |
| Long             | `long`                        | `BasicType.LONG_TYPE`                 | `bigint`               | `java.lang.Long`           | -                                                         |
| Unsigned Long    | `long unsigned`               | `BasicType.LONG_TYPE`                 | `bigint`               | `java.lang.Long`           | Unsigned flag is ignored                                  |
| Float            | `float`                       | `BasicType.FLOAT_TYPE`                | `float`                | `java.lang.Float`          | Single-precision floating point                           |
| Double           | `double`                      | `BasicType.DOUBLE_TYPE`               | `double`               | `java.lang.Double`         | Double-precision floating point                           |
| Decimal          | `decimal(p, s)`               | `DecimalType(p, s)`                   | `"decimal(p,s)"`       | `java.math.BigDecimal`     | Precision: 1-38, Scale: 0-precision                       |
| String           | `string`                      | `BasicType.STRING_TYPE`               | `string`               | `java.lang.String`         | Variable-length string                                    |
| FixedChar        | `char(l)`                     | `BasicType.STRING_TYPE`               | `string`               | `java.lang.String`         | Fixed-length string, length stored in columnLength        |
| VarChar          | `varchar(l)`                  | `BasicType.STRING_TYPE`               | `string`               | `java.lang.String`         | Variable-length string, max length stored in columnLength |
| UUID             | `uuid`                        | `BasicType.STRING_TYPE`               | `string`               | `java.lang.String`         | Universally unique identifier                             |
| Date             | `date`                        | `LocalTimeType.LOCAL_DATE_TYPE`       | `date`                 | `java.time.LocalDate`      | Date without time                                         |
| Time             | `time`                        | `LocalTimeType.LOCAL_TIME_TYPE`       | `time`                 | `java.time.LocalTime`      | Time without date                                         |
| Timestamp        | `timestamp(p)`                | `LocalTimeType.LOCAL_DATE_TIME_TYPE`  | `timestamp`            | `java.time.LocalDateTime`  | Timestamp without timezone, p=0-12                        |
| TimestampTz      | `timestamp_tz(p)`             | `LocalTimeType.OFFSET_DATE_TIME_TYPE` | `timestamp_tz`         | `java.time.OffsetDateTime` | Timestamp with timezone, p=0-12                           |
| Binary           | `binary`                      | `PrimitiveByteArrayType.INSTANCE`     | `bytes`                | `byte[]`                   | Variable-length binary                                    |
| Fixed            | `fixed(l)`                    | `PrimitiveByteArrayType.INSTANCE`     | `bytes`                | `byte[]`                   | Fixed-length binary                                       |
| IntervalYear     | `interval_year`               | `BasicType.STRING_TYPE`               | `string`               | `java.lang.String`         | Year-month interval                                       |
| IntervalDay      | `interval_day`                | `BasicType.STRING_TYPE`               | `string`               | `java.lang.String`         | Day-time interval                                         |

## Complex Type Mapping

| Gravitino Type | Gravitino JSON Representation                                                       | SeaTunnel Type          | SeaTunnel Type Keyword              | Notes                                       |
|:---------------|:------------------------------------------------------------------------------------|:------------------------|:------------------------------------|:--------------------------------------------|
| List           | `{"type": "list", "elementType": type, "containsNull": boolean}`                    | `ArrayType`             | `"array<T>"`                        | T is the element type                       |
| Map            | `{"type": "map", "keyType": type, "valueType": type, "valueContainsNull": boolean}` | `MapType`               | `"map<K,V>"`                        | K is key type, V is value type              |
| Struct         | `{"type": "struct", "fields": [...]}`                                               | `SeaTunnelRowType`      | `{field1=type1, field2=type2, ...}` | Nested row type                             |
| External       | `{"type": "external", "catalogString": "user-defined"}`                             | `BasicType.STRING_TYPE` | `string`                            | For unsupported types like PostgreSQL jsonb |
| Union          | `{"type": "union", "types": [...]}`                                                 | Not Supported           | -                                   | Throws conversion error                     |

## Type Parameter Extraction

The converter extracts type parameters for column metadata:

| Type              | Parameter        | Extracted As                        | Notes                               |
|:------------------|:-----------------|:------------------------------------|:------------------------------------|
| `decimal(p, s)`   | precision, scale | columnLength=precision, scale=scale | Both values stored                  |
| `varchar(l)`      | length           | columnLength=length                 | Maximum string length               |
| `char(l)`         | length           | columnLength=length                 | Fixed string length                 |
| `fixed(l)`        | length           | columnLength=length                 | Fixed binary length                 |
| `timestamp(p)`    | precision        | columnLength=precision              | Fractional seconds precision (0-12) |
| `timestamp_tz(p)` | precision        | columnLength=precision              | Fractional seconds precision (0-12) |

## Index and Constraint Mapping

Gravitino indexes are mapped to SeaTunnel constraints:

| Gravitino Index Type | SeaTunnel Constraint Type  | Notes                                       |
|:---------------------|:---------------------------|:--------------------------------------------|
| `PRIMARY_KEY`        | `PrimaryKey`               | Extracts column names from fieldNames array |
| `UNIQUE_KEY`         | `ConstraintKey.UNIQUE_KEY` | Column sort order defaults to ASC           |

## Notes and Limitations

1. **Case Insensitivity**: Type matching is case-insensitive. `BOOLEAN`, `boolean`, and `Boolean` are treated the same.

2. **Unsigned Types**: The `unsigned` modifier for numeric types is recognized but does not affect the converted SeaTunnel type. SeaTunnel uses signed types internally.

3. **External Types**: When Gravitino encounters a type it cannot parse (such as PostgreSQL's `jsonb`), it represents it as an `external` type. SeaTunnel converts these to `string` type.

4. **Union Types**: Gravitino's `union` type is not currently supported and will throw a conversion error.

5. **Nullable**: The `nullable` attribute in Gravitino column definitions is preserved in the SeaTunnel `Column` metadata.

6. **Decimal Parameters**: The `decimal` type requires both precision and scale parameters. Decimal values without parameters or with invalid format will throw an error.

## Related Documentation

- [Gravitino Column Types](https://gravitino.apache.org/docs/1.1.0/manage-relational-metadata-using-gravitino/#apache-gravitino-table-column-type)
- [Schema Feature](./schema-feature.md)
- SeaTunnel Data Types
- [SeaTunnel Data Types](./schema-feature.md)

# Gravitino 类型映射

本文档描述了使用 Apache Gravitino 作为元数据源时，Gravitino 与 SeaTunnel 之间的类型映射关系。类型转换由 `GravitinoTableSchemaConvertor` 处理。

## 概述

当 SeaTunnel 从 Gravitino 读取表结构时，Gravitino 的列类型会自动转换为对应的 SeaTunnel 数据类型。这种映射使得 Gravitino 管理的元数据能够无缝集成到 SeaTunnel 的数据处理管道中。

## 基础类型映射

| Gravitino 类型     | Gravitino JSON 表示  | SeaTunnel 类型                          | SeaTunnel 类型关键字  | Java 类型                    | 说明                        |
|:-----------------|:-------------------|:--------------------------------------|:-----------------|:---------------------------|:--------------------------|
| Boolean          | `boolean`          | `BasicType.BOOLEAN_TYPE`              | `boolean`        | `java.lang.Boolean`        | 布尔类型                      |
| Byte             | `byte`             | `BasicType.BYTE_TYPE`                 | `tinyint`        | `java.lang.Byte`           | 1字节整数                     |
| Unsigned Byte    | `byte unsigned`    | `BasicType.BYTE_TYPE`                 | `tinyint`        | `java.lang.Byte`           | 无符号字节（unsigned标志被忽略）      |
| Short            | `short`            | `BasicType.SHORT_TYPE`                | `smallint`       | `java.lang.Short`          | 2字节整数                     |
| Unsigned Short   | `short unsigned`   | `BasicType.SHORT_TYPE`                | `smallint`       | `java.lang.Short`          | 无符号短整型（unsigned标志被忽略）     |
| Integer          | `integer`          | `BasicType.INT_TYPE`                  | `int`            | `java.lang.Integer`        | 4字节整数                     |
| Unsigned Integer | `integer unsigned` | `BasicType.INT_TYPE`                  | `int`            | `java.lang.Integer`        | 无符号整型（unsigned标志被忽略）      |
| Long             | `long`             | `BasicType.LONG_TYPE`                 | `bigint`         | `java.lang.Long`           | 8字节整数                     |
| Unsigned Long    | `long unsigned`    | `BasicType.LONG_TYPE`                 | `bigint`         | `java.lang.Long`           | 无符号长整型（unsigned标志被忽略）     |
| Float            | `float`            | `BasicType.FLOAT_TYPE`                | `float`          | `java.lang.Float`          | 单精度浮点数                    |
| Double           | `double`           | `BasicType.DOUBLE_TYPE`               | `double`         | `java.lang.Double`         | 双精度浮点数                    |
| Decimal          | `decimal(p, s)`    | `DecimalType(p, s)`                   | `"decimal(p,s)"` | `java.math.BigDecimal`     | 精度: 1-38, 小数位: 0-精度       |
| String           | `string`           | `BasicType.STRING_TYPE`               | `string`         | `java.lang.String`         | 变长字符串                     |
| FixedChar        | `char(l)`          | `BasicType.STRING_TYPE`               | `string`         | `java.lang.String`         | 定长字符串，长度存储在columnLength   |
| VarChar          | `varchar(l)`       | `BasicType.STRING_TYPE`               | `string`         | `java.lang.String`         | 变长字符串，最大长度存储在columnLength |
| UUID             | `uuid`             | `BasicType.STRING_TYPE`               | `string`         | `java.lang.String`         | 通用唯一标识符                   |
| Date             | `date`             | `LocalTimeType.LOCAL_DATE_TYPE`       | `date`           | `java.time.LocalDate`      | 日期（不含时间）                  |
| Time             | `time`             | `LocalTimeType.LOCAL_TIME_TYPE`       | `time`           | `java.time.LocalTime`      | 时间（不含日期）                  |
| Timestamp        | `timestamp(p)`     | `LocalTimeType.LOCAL_DATE_TIME_TYPE`  | `timestamp`      | `java.time.LocalDateTime`  | 不带时区的时间戳，p=0-12           |
| TimestampTz      | `timestamp_tz(p)`  | `LocalTimeType.OFFSET_DATE_TIME_TYPE` | `timestamp_tz`   | `java.time.OffsetDateTime` | 带时区的时间戳，p=0-12            |
| Binary           | `binary`           | `PrimitiveByteArrayType.INSTANCE`     | `bytes`          | `byte[]`                   | 变长二进制数据                   |
| Fixed            | `fixed(l)`         | `PrimitiveByteArrayType.INSTANCE`     | `bytes`          | `byte[]`                   | 定长二进制数据                   |
| IntervalYear     | `interval_year`    | `BasicType.STRING_TYPE`               | `string`         | `java.lang.String`         | 年-月间隔                     |
| IntervalDay      | `interval_day`     | `BasicType.STRING_TYPE`               | `string`         | `java.lang.String`         | 日-时间隔                     |

## 复杂类型映射

| Gravitino 类型 | Gravitino JSON 表示                                                                   | SeaTunnel 类型            | SeaTunnel 类型关键字                     | 说明                        |
|:-------------|:------------------------------------------------------------------------------------|:------------------------|:------------------------------------|:--------------------------|
| List         | `{"type": "list", "elementType": type, "containsNull": boolean}`                    | `ArrayType`             | `"array<T>"`                        | T为元素类型                    |
| Map          | `{"type": "map", "keyType": type, "valueType": type, "valueContainsNull": boolean}` | `MapType`               | `"map<K,V>"`                        | K为键类型，V为值类型               |
| Struct       | `{"type": "struct", "fields": [...]}`                                               | `SeaTunnelRowType`      | `{field1=type1, field2=type2, ...}` | 嵌套行类型                     |
| External     | `{"type": "external", "catalogString": "user-defined"}`                             | `BasicType.STRING_TYPE` | `string`                            | 不支持的类型（如PostgreSQL的jsonb） |
| Union        | `{"type": "union", "types": [...]}`                                                 | 不支持                     | -                                   | 抛出转换错误                    |

## 类型参数提取

转换器会提取类型参数作为列元数据：

| 类型                | 参数               | 提取为                                 | 说明          |
|:------------------|:-----------------|:------------------------------------|:------------|
| `decimal(p, s)`   | precision, scale | columnLength=precision, scale=scale | 两个值都会存储     |
| `varchar(l)`      | length           | columnLength=length                 | 字符串最大长度     |
| `char(l)`         | length           | columnLength=length                 | 定长字符串长度     |
| `fixed(l)`        | length           | columnLength=length                 | 定长二进制长度     |
| `timestamp(p)`    | precision        | columnLength=precision              | 小数秒精度（0-12） |
| `timestamp_tz(p)` | precision        | columnLength=precision              | 小数秒精度（0-12） |

## 索引和约束映射

Gravitino 索引映射到 SeaTunnel 约束：

| Gravitino 索引类型 | SeaTunnel 约束类型             | 说明                  |
|:---------------|:---------------------------|:--------------------|
| `PRIMARY_KEY`  | `PrimaryKey`               | 从 fieldNames 数组提取列名 |
| `UNIQUE_KEY`   | `ConstraintKey.UNIQUE_KEY` | 列排序顺序默认为 ASC        |

## 注意事项和限制

1. **大小写不敏感**：类型匹配不区分大小写。`BOOLEAN`、`boolean` 和 `Boolean` 被视为相同。

2. **无符号类型**：数值类型的 `unsigned` 修饰符会被识别，但不影响转换后的 SeaTunnel 类型。SeaTunnel 内部使用有符号类型。

3. **外部类型**：当 Gravitino 遇到无法解析的类型（如 PostgreSQL 的 `jsonb`）时，会将其表示为 `external` 类型。SeaTunnel 会将其转换为 `string` 类型。

4. **联合类型**：Gravitino 的 `union` 类型目前不支持，会抛出转换错误。

5. **可空性**：Gravitino 列定义中的 `nullable` 属性会保留在 SeaTunnel `Column` 元数据中。

6. **Decimal 参数**：`decimal` 类型必须同时指定精度和小数位参数。没有参数或格式无效的 decimal 值会抛出错误。

## 相关文档

- [Gravitino 列类型](https://gravitino.apache.org/docs/1.1.0/manage-relational-metadata-using-gravitino/#apache-gravitino-table-column-type)
- [Schema 特性](./schema-feature.md)
- [SeaTunnel 数据类型](../common-options.md)

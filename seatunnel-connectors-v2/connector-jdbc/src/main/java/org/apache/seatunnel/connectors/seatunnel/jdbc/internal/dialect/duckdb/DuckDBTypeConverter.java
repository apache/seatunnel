/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class DuckDBTypeConverter implements TypeConverter<BasicTypeDefine> {

    // Boolean
    public static final String DUCKDB_BOOLEAN = "BOOLEAN";

    // Numeric
    public static final String DUCKDB_TINYINT = "TINYINT";
    public static final String DUCKDB_SMALLINT = "SMALLINT";
    public static final String DUCKDB_INTEGER = "INTEGER";
    public static final String DUCKDB_BIGINT = "BIGINT";
    public static final String DUCKDB_HUGEINT = "HUGEINT";
    public static final String DUCKDB_BIGNUM = "BIGNUM";
    public static final String DUCKDB_UHUGEINT = "UHUGEINT";
    public static final String DUCKDB_UTINYINT = "UTINYINT";
    public static final String DUCKDB_USMALLINT = "USMALLINT";
    public static final String DUCKDB_UINTEGER = "UINTEGER";
    public static final String DUCKDB_UBIGINT = "UBIGINT";
    public static final String DUCKDB_DECIMAL = "DECIMAL";
    public static final String DUCKDB_FLOAT = "FLOAT";
    public static final String DUCKDB_DOUBLE = "DOUBLE";

    // String / binary
    public static final String DUCKDB_BIT = "BIT";
    public static final String DUCKDB_VARCHAR = "VARCHAR";
    public static final String DUCKDB_CHAR = "CHAR";
    public static final String DUCKDB_BPCHAR = "BPCHAR";
    public static final String DUCKDB_STRING = "STRING";
    public static final String DUCKDB_TEXT = "TEXT";
    public static final String DUCKDB_BLOB = "BLOB";
    public static final String DUCKDB_UUID = "UUID";
    public static final String DUCKDB_JSON = "JSON";

    // Temporal
    public static final String DUCKDB_DATE = "DATE";
    public static final String DUCKDB_TIME = "TIME";
    public static final String DUCKDB_TIMESTAMP = "TIMESTAMP";
    public static final String DUCKDB_TIMESTAMP_WITH_TZ = "TIMESTAMP WITH TIME ZONE";

    // Other
    public static final String DUCKDB_INTERVAL = "INTERVAL";
    public static final String DUCKDB_ARRAY = "ARRAY";
    public static final String DUCKDB_STRUCT = "STRUCT";
    public static final String DUCKDB_MAP = "MAP";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = 18;
    public static final int MAX_SCALE = 38;
    public static final int DEFAULT_SCALE = 3;

    public static final DuckDBTypeConverter INSTANCE = new DuckDBTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.DUCKDB;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String duckDBType = typeDefine.getDataType().toUpperCase();
        Long length = typeDefine.getLength();
        long lengthValue = length == null ? 0L : length;
        switch (duckDBType) {
            case DUCKDB_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DUCKDB_TINYINT:
            case DUCKDB_UTINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case DUCKDB_SMALLINT:
            case DUCKDB_USMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case DUCKDB_INTEGER:
            case DUCKDB_UINTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DUCKDB_BIGINT:
            case DUCKDB_UBIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DUCKDB_HUGEINT:
            case DUCKDB_UHUGEINT:
            case DUCKDB_BIGNUM:
                builder.dataType(new DecimalType(MAX_PRECISION, 0));
                builder.columnLength((long) MAX_PRECISION);
                break;
            case DUCKDB_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DUCKDB_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DUCKDB_DECIMAL:
                handleDecimalType(builder, typeDefine);
                break;
            case DUCKDB_VARCHAR:
            case DUCKDB_TEXT:
            case DUCKDB_CHAR:
            case DUCKDB_BPCHAR:
            case DUCKDB_STRING:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(length);
                break;
            case DUCKDB_BIT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(lengthValue > 0 ? lengthValue : 1L);
                break;
            case DUCKDB_UUID:
            case DUCKDB_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(lengthValue > 0 ? lengthValue : 255);
                break;
            case DUCKDB_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(length);
                break;
            case DUCKDB_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DUCKDB_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case DUCKDB_TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case DUCKDB_TIMESTAMP_WITH_TZ:
                builder.dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE);
                break;
            case DUCKDB_INTERVAL:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(50L);
                break;
            case DUCKDB_ARRAY:
            case DUCKDB_STRUCT:
            case DUCKDB_MAP:
                log.warn(
                        "Complex type {} mapped to STRING, consider using JSON serialization",
                        duckDBType);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(lengthValue > 0 ? lengthValue : 65535);
                break;
            default:
                log.warn("Unsupported DuckDB type: {}, falling back to STRING", duckDBType);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(lengthValue > 0 ? lengthValue : 255);
        }
        return builder.build();
    }

    private void handleDecimalType(
            PhysicalColumn.PhysicalColumnBuilder builder, BasicTypeDefine typeDefine) {
        long precision =
                typeDefine.getPrecision() != null ? typeDefine.getPrecision() : DEFAULT_PRECISION;
        int scale = typeDefine.getScale() != null ? typeDefine.getScale() : DEFAULT_SCALE;

        if (precision > MAX_PRECISION) {
            log.warn(
                    "DECIMAL precision {} exceeds maximum {}, truncating to {}",
                    precision,
                    MAX_PRECISION,
                    MAX_PRECISION);
            precision = MAX_PRECISION;
        }
        if (scale < 0) {
            log.warn("DECIMAL scale {} is negative, setting to 0", scale);
            scale = 0;
        } else if (scale > MAX_SCALE) {
            log.warn(
                    "DECIMAL scale {} exceeds maximum {}, truncating to {}",
                    scale,
                    MAX_SCALE,
                    MAX_SCALE);
            scale = MAX_SCALE;
        }

        if (scale <= 0) {
            builder.dataType(new DecimalType((int) precision, 0));
        } else {
            builder.dataType(new DecimalType((int) precision, scale));
        }
        builder.columnLength(precision);
        builder.scale(scale);
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(DUCKDB_BOOLEAN);
                builder.dataType(DUCKDB_BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(DUCKDB_TINYINT);
                builder.dataType(DUCKDB_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(DUCKDB_SMALLINT);
                builder.dataType(DUCKDB_SMALLINT);
                break;
            case INT:
                builder.columnType(DUCKDB_INTEGER);
                builder.dataType(DUCKDB_INTEGER);
                break;
            case BIGINT:
                builder.columnType(DUCKDB_BIGINT);
                builder.dataType(DUCKDB_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DUCKDB_FLOAT);
                builder.dataType(DUCKDB_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(DUCKDB_DOUBLE);
                builder.dataType(DUCKDB_DOUBLE);
                break;
            case DECIMAL:
                reconvertDecimalType(column, builder);
                break;
            case STRING:
                builder.columnType(DUCKDB_VARCHAR);
                builder.dataType(DUCKDB_VARCHAR);
                builder.length(column.getColumnLength());
                break;
            case DATE:
                builder.columnType(DUCKDB_DATE);
                builder.dataType(DUCKDB_DATE);
                break;
            case TIME:
                builder.columnType(DUCKDB_TIME);
                builder.dataType(DUCKDB_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(DUCKDB_TIMESTAMP);
                builder.dataType(DUCKDB_TIMESTAMP);
                break;
            case TIMESTAMP_TZ:
                builder.columnType(DUCKDB_TIMESTAMP_WITH_TZ);
                builder.dataType(DUCKDB_TIMESTAMP_WITH_TZ);
                break;
            case BYTES:
                builder.columnType(DUCKDB_BLOB);
                builder.dataType(DUCKDB_BLOB);
                builder.length(column.getColumnLength());
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DUCKDB,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private void reconvertDecimalType(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        DecimalType decimalType = (DecimalType) column.getDataType();
        long precision =
                decimalType.getPrecision() > 0 ? decimalType.getPrecision() : DEFAULT_PRECISION;
        int scale = decimalType.getScale();
        if (precision > MAX_PRECISION) {
            log.warn(
                    "DECIMAL precision {} exceeds maximum {}, truncating to {}",
                    precision,
                    MAX_PRECISION,
                    MAX_PRECISION);
            precision = MAX_PRECISION;
        }
        if (scale < 0) {
            log.warn("DECIMAL scale {} is negative, setting to 0", scale);
            scale = 0;
        } else if (scale > MAX_SCALE) {
            log.warn(
                    "DECIMAL scale {} exceeds maximum {}, truncating to {}",
                    scale,
                    MAX_SCALE,
                    MAX_SCALE);
            scale = MAX_SCALE;
        }
        builder.columnType(String.format("%s(%d,%d)", DUCKDB_DECIMAL, precision, scale));
        builder.dataType(DUCKDB_DECIMAL);
        builder.precision(precision);
        builder.scale(scale);
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference https://learn.microsoft.com/zh-cn/sql/t-sql/data-types/data-types-transact-sql
@Slf4j
@AutoService(TypeConverter.class)
public class SqlServerTypeConverter implements TypeConverter<BasicTypeDefine> {
    // -------------------------number----------------------------
    public static final String SQLSERVER_BIT = "BIT";
    public static final String SQLSERVER_TINYINT = "TINYINT";
    public static final String SQLSERVER_TINYINT_IDENTITY = "TINYINT IDENTITY";
    public static final String SQLSERVER_SMALLINT = "SMALLINT";
    public static final String SQLSERVER_SMALLINT_IDENTITY = "SMALLINT IDENTITY";
    public static final String SQLSERVER_INTEGER = "INTEGER";
    public static final String SQLSERVER_INTEGER_IDENTITY = "INTEGER IDENTITY";
    public static final String SQLSERVER_INT = "INT";
    private static final String SQLSERVER_INT_IDENTITY = "INT IDENTITY";
    public static final String SQLSERVER_BIGINT = "BIGINT";
    public static final String SQLSERVER_BIGINT_IDENTITY = "BIGINT IDENTITY";
    public static final String SQLSERVER_DECIMAL = "DECIMAL";
    public static final String SQLSERVER_FLOAT = "FLOAT";
    public static final String SQLSERVER_REAL = "REAL";
    public static final String SQLSERVER_NUMERIC = "NUMERIC";
    public static final String SQLSERVER_MONEY = "MONEY";
    public static final String SQLSERVER_SMALLMONEY = "SMALLMONEY";
    // -------------------------string----------------------------
    public static final String SQLSERVER_CHAR = "CHAR";
    public static final String SQLSERVER_VARCHAR = "VARCHAR";
    public static final String SQLSERVER_NCHAR = "NCHAR";
    public static final String SQLSERVER_NVARCHAR = "NVARCHAR";
    public static final String SQLSERVER_TEXT = "TEXT";
    public static final String SQLSERVER_NTEXT = "NTEXT";
    public static final String SQLSERVER_XML = "XML";
    public static final String SQLSERVER_UNIQUEIDENTIFIER = "UNIQUEIDENTIFIER";
    public static final String SQLSERVER_SQLVARIANT = "SQL_VARIANT";
    // ------------------------------time-------------------------
    public static final String SQLSERVER_DATE = "DATE";
    public static final String SQLSERVER_TIME = "TIME";
    public static final String SQLSERVER_DATETIME = "DATETIME";
    public static final String SQLSERVER_DATETIME2 = "DATETIME2";
    public static final String SQLSERVER_SMALLDATETIME = "SMALLDATETIME";
    public static final String SQLSERVER_DATETIMEOFFSET = "DATETIMEOFFSET";
    public static final String SQLSERVER_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    public static final String SQLSERVER_BINARY = "BINARY";
    public static final String SQLSERVER_VARBINARY = "VARBINARY";
    public static final String SQLSERVER_IMAGE = "IMAGE";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_CHAR_LENGTH = 8000;
    public static final int MAX_NVARCHAR_LENGTH = 4000;
    public static final int MAX_BINARY_LENGTH = 8000;
    public static final int MAX_TIME_SCALE = 7;
    public static final int MAX_TIMESTAMP_SCALE = 7;
    public static final String MAX_VARBINARY = String.format("%s(%s)", SQLSERVER_VARBINARY, "MAX");
    public static final String MAX_VARCHAR = String.format("%s(%s)", SQLSERVER_VARCHAR, "MAX");

    public static final String MAX_NVARCHAR = String.format("%s(%s)", SQLSERVER_NVARCHAR, "MAX");
    public static final long POWER_2_30 = (long) Math.pow(2, 30);
    public static final long POWER_2_31 = (long) Math.pow(2, 31);
    public static final SqlServerTypeConverter INSTANCE = new SqlServerTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String sqlServerType = typeDefine.getDataType().toUpperCase();
        switch (sqlServerType) {
            case SQLSERVER_BIT:
                builder.sourceType(SQLSERVER_BIT);
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case SQLSERVER_TINYINT:
            case SQLSERVER_TINYINT_IDENTITY:
                builder.sourceType(SQLSERVER_TINYINT);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case SQLSERVER_SMALLINT:
            case SQLSERVER_SMALLINT_IDENTITY:
                builder.sourceType(SQLSERVER_SMALLINT);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case SQLSERVER_INTEGER:
            case SQLSERVER_INTEGER_IDENTITY:
            case SQLSERVER_INT:
            case SQLSERVER_INT_IDENTITY:
                builder.sourceType(SQLSERVER_INT);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case SQLSERVER_BIGINT:
            case SQLSERVER_BIGINT_IDENTITY:
                builder.sourceType(SQLSERVER_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case SQLSERVER_REAL:
                builder.sourceType(SQLSERVER_REAL);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case SQLSERVER_FLOAT:
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() <= 24) {
                    builder.sourceType(SQLSERVER_REAL);
                    builder.dataType(BasicType.FLOAT_TYPE);
                } else {
                    builder.sourceType(SQLSERVER_FLOAT);
                    builder.dataType(BasicType.DOUBLE_TYPE);
                }
                break;
            case SQLSERVER_DECIMAL:
            case SQLSERVER_NUMERIC:
                builder.sourceType(
                        String.format(
                                "%s(%s,%s)",
                                SQLSERVER_DECIMAL,
                                typeDefine.getPrecision(),
                                typeDefine.getScale()));
                builder.dataType(
                        new DecimalType(
                                typeDefine.getPrecision().intValue(), typeDefine.getScale()));
                builder.columnLength(typeDefine.getPrecision());
                builder.scale(typeDefine.getScale());
                break;
            case SQLSERVER_MONEY:
                builder.sourceType(SQLSERVER_MONEY);
                builder.dataType(
                        new DecimalType(
                                typeDefine.getPrecision().intValue(), typeDefine.getScale()));
                builder.columnLength(typeDefine.getPrecision());
                builder.scale(typeDefine.getScale());
                break;
            case SQLSERVER_SMALLMONEY:
                builder.sourceType(SQLSERVER_SMALLMONEY);
                builder.dataType(
                        new DecimalType(
                                typeDefine.getPrecision().intValue(), typeDefine.getScale()));
                builder.columnLength(typeDefine.getPrecision());
                builder.scale(typeDefine.getScale());
                break;
            case SQLSERVER_CHAR:
                builder.sourceType(String.format("%s(%s)", SQLSERVER_CHAR, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(
                        TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                break;
            case SQLSERVER_NCHAR:
                builder.sourceType(
                        String.format("%s(%s)", SQLSERVER_NCHAR, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(
                        TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                break;
            case SQLSERVER_VARCHAR:
                if (typeDefine.getLength() == -1) {
                    builder.sourceType(MAX_VARCHAR);
                    builder.columnLength(TypeDefineUtils.doubleByteTo4ByteLength(POWER_2_31 - 1));
                } else {
                    builder.sourceType(
                            String.format("%s(%s)", SQLSERVER_VARCHAR, typeDefine.getLength()));
                    builder.columnLength(
                            TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case SQLSERVER_NVARCHAR:
                if (typeDefine.getLength() == -1) {
                    builder.sourceType(MAX_NVARCHAR);
                    builder.columnLength(TypeDefineUtils.doubleByteTo4ByteLength(POWER_2_31 - 1));
                } else {
                    builder.sourceType(
                            String.format("%s(%s)", SQLSERVER_NVARCHAR, typeDefine.getLength()));
                    builder.columnLength(
                            TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case SQLSERVER_TEXT:
                builder.sourceType(SQLSERVER_TEXT);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_31 - 1);
                break;
            case SQLSERVER_NTEXT:
                builder.sourceType(SQLSERVER_NTEXT);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_30 - 1);
                break;
            case SQLSERVER_XML:
                builder.sourceType(SQLSERVER_XML);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_31 - 1);
                break;
            case SQLSERVER_UNIQUEIDENTIFIER:
                builder.sourceType(SQLSERVER_UNIQUEIDENTIFIER);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case SQLSERVER_SQLVARIANT:
                builder.sourceType(SQLSERVER_SQLVARIANT);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case SQLSERVER_BINARY:
                builder.sourceType(
                        String.format("%s(%s)", SQLSERVER_BINARY, typeDefine.getLength()));
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case SQLSERVER_VARBINARY:
                if (typeDefine.getLength() == -1) {
                    builder.sourceType(MAX_VARBINARY);
                    builder.columnLength(POWER_2_31 - 1);
                } else {
                    builder.sourceType(
                            String.format("%s(%s)", SQLSERVER_VARBINARY, typeDefine.getLength()));
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case SQLSERVER_IMAGE:
                builder.sourceType(SQLSERVER_IMAGE);
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_31 - 1);
                break;
            case SQLSERVER_TIMESTAMP:
                builder.sourceType(SQLSERVER_TIMESTAMP);
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(8L);
                break;
            case SQLSERVER_DATE:
                builder.sourceType(SQLSERVER_DATE);
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case SQLSERVER_TIME:
                builder.sourceType(String.format("%s(%s)", SQLSERVER_TIME, typeDefine.getScale()));
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case SQLSERVER_DATETIME:
                builder.sourceType(SQLSERVER_DATETIME);
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(3);
                break;
            case SQLSERVER_DATETIME2:
                builder.sourceType(
                        String.format("%s(%s)", SQLSERVER_DATETIME2, typeDefine.getScale()));
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case SQLSERVER_DATETIMEOFFSET:
                builder.sourceType(
                        String.format("%s(%s)", SQLSERVER_DATETIMEOFFSET, typeDefine.getScale()));
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case SQLSERVER_SMALLDATETIME:
                builder.sourceType(SQLSERVER_SMALLDATETIME);
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SQLSERVER, sqlServerType, typeDefine.getName());
        }
        return builder.build();
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
                builder.columnType(SQLSERVER_BIT);
                builder.dataType(SQLSERVER_BIT);
                break;
            case TINYINT:
                builder.columnType(SQLSERVER_TINYINT);
                builder.dataType(SQLSERVER_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(SQLSERVER_SMALLINT);
                builder.dataType(SQLSERVER_SMALLINT);
                break;
            case INT:
                builder.columnType(SQLSERVER_INT);
                builder.dataType(SQLSERVER_INT);
                break;
            case BIGINT:
                builder.columnType(SQLSERVER_BIGINT);
                builder.dataType(SQLSERVER_BIGINT);
                break;
            case FLOAT:
                builder.columnType(SQLSERVER_REAL);
                builder.dataType(SQLSERVER_REAL);
                break;
            case DOUBLE:
                builder.columnType(SQLSERVER_FLOAT);
                builder.dataType(SQLSERVER_FLOAT);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                    scale = DEFAULT_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is precision less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (precision > MAX_PRECISION) {
                    scale = (int) Math.max(0, scale - (precision - MAX_PRECISION));
                    precision = MAX_PRECISION;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION,
                            precision,
                            scale);
                }
                if (scale < 0) {
                    scale = 0;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is scale less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_SCALE,
                            precision,
                            scale);
                }
                builder.columnType(String.format("%s(%s,%s)", SQLSERVER_DECIMAL, precision, scale));
                builder.dataType(SQLSERVER_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(MAX_NVARCHAR);
                    builder.dataType(MAX_NVARCHAR);
                } else if (column.getColumnLength() <= MAX_NVARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", SQLSERVER_NVARCHAR, column.getColumnLength()));
                    builder.dataType(SQLSERVER_NVARCHAR);
                    builder.length(column.getColumnLength());
                } else {
                    builder.columnType(MAX_NVARCHAR);
                    builder.dataType(MAX_NVARCHAR);
                    builder.length(column.getColumnLength());
                }
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(MAX_VARBINARY);
                    builder.dataType(SQLSERVER_VARBINARY);
                } else if (column.getColumnLength() <= MAX_BINARY_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", SQLSERVER_VARBINARY, column.getColumnLength()));
                    builder.dataType(SQLSERVER_VARBINARY);
                    builder.length(column.getColumnLength());
                } else {
                    builder.columnType(MAX_VARBINARY);
                    builder.dataType(SQLSERVER_VARBINARY);
                    builder.length(column.getColumnLength());
                }
                break;
            case DATE:
                builder.columnType(SQLSERVER_DATE);
                builder.dataType(SQLSERVER_DATE);
                break;
            case TIME:
                if (column.getScale() != null && column.getScale() > 0) {
                    int timeScale = column.getScale();
                    if (timeScale > MAX_TIME_SCALE) {
                        timeScale = MAX_TIME_SCALE;
                        log.warn(
                                "The time column {} type time({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to time({})",
                                column.getName(),
                                column.getScale(),
                                MAX_SCALE,
                                timeScale);
                    }
                    builder.columnType(String.format("%s(%s)", SQLSERVER_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(SQLSERVER_TIME);
                }
                builder.dataType(SQLSERVER_TIME);
                break;
            case TIMESTAMP:
                if (column.getScale() != null && column.getScale() > 0) {
                    int timestampScale = column.getScale();
                    if (timestampScale > MAX_TIMESTAMP_SCALE) {
                        timestampScale = MAX_TIMESTAMP_SCALE;
                        log.warn(
                                "The timestamp column {} type timestamp({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to timestamp({})",
                                column.getName(),
                                column.getScale(),
                                MAX_TIMESTAMP_SCALE,
                                timestampScale);
                    }
                    builder.columnType(
                            String.format("%s(%s)", SQLSERVER_DATETIME2, timestampScale));
                    builder.scale(timestampScale);
                } else {
                    builder.columnType(SQLSERVER_DATETIME2);
                }
                builder.dataType(SQLSERVER_DATETIME2);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.SQLSERVER,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}

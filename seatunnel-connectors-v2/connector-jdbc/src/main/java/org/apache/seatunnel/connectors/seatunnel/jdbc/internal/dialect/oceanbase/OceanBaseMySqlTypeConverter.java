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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class OceanBaseMySqlTypeConverter
        implements TypeConverter<BasicTypeDefine<OceanBaseMysqlType>> {

    // ============================data types=====================
    static final String MYSQL_NULL = "NULL";
    static final String MYSQL_BIT = "BIT";

    // -------------------------number----------------------------
    static final String MYSQL_TINYINT = "TINYINT";
    static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    static final String MYSQL_SMALLINT = "SMALLINT";
    static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    static final String MYSQL_INT = "INT";
    static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    static final String MYSQL_INTEGER = "INTEGER";
    static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    static final String MYSQL_BIGINT = "BIGINT";
    static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    static final String MYSQL_DECIMAL = "DECIMAL";
    static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    static final String MYSQL_FLOAT = "FLOAT";
    static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    static final String MYSQL_DOUBLE = "DOUBLE";
    static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    public static final String MYSQL_CHAR = "CHAR";
    public static final String MYSQL_VARCHAR = "VARCHAR";
    static final String MYSQL_TINYTEXT = "TINYTEXT";
    static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    static final String MYSQL_TEXT = "TEXT";
    static final String MYSQL_LONGTEXT = "LONGTEXT";
    static final String MYSQL_JSON = "JSON";
    static final String MYSQL_ENUM = "ENUM";

    // ------------------------------time-------------------------
    static final String MYSQL_DATE = "DATE";
    public static final String MYSQL_DATETIME = "DATETIME";
    public static final String MYSQL_TIME = "TIME";
    public static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    static final String MYSQL_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    static final String MYSQL_TINYBLOB = "TINYBLOB";
    static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    static final String MYSQL_BLOB = "BLOB";
    static final String MYSQL_LONGBLOB = "LONGBLOB";
    static final String MYSQL_BINARY = "BINARY";
    static final String MYSQL_VARBINARY = "VARBINARY";
    static final String MYSQL_GEOMETRY = "GEOMETRY";

    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_PRECISION = 65;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_SCALE = 30;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final long POWER_2_8 = (long) Math.pow(2, 8);
    public static final long POWER_2_16 = (long) Math.pow(2, 16);
    public static final long POWER_2_24 = (long) Math.pow(2, 24);
    public static final long POWER_2_32 = (long) Math.pow(2, 32);
    public static final long MAX_VARBINARY_LENGTH = POWER_2_16 - 4;

    private static final String VECTOR_TYPE_NAME = "";
    private static final String VECTOR_NAME = "VECTOR";

    @Override
    public String identifier() {
        return DatabaseIdentifier.OCENABASE;
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

        String mysqlDataType = typeDefine.getDataType().toUpperCase();
        if (typeDefine.isUnsigned() && !(mysqlDataType.endsWith(" UNSIGNED"))) {
            mysqlDataType = mysqlDataType + " UNSIGNED";
        }
        System.out.println(typeDefine.getName() + "的值类型是：" + mysqlDataType);
        switch (mysqlDataType) {
            case MYSQL_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case MYSQL_BIT:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else if (typeDefine.getLength() == 1) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    // BIT(M) -> BYTE(M/8)
                    long byteLength = typeDefine.getLength() / 8;
                    byteLength += typeDefine.getLength() % 8 > 0 ? 1 : 0;
                    builder.columnLength(byteLength);
                }
                break;
            case MYSQL_TINYINT:
                if (typeDefine.getColumnType().equalsIgnoreCase("tinyint(1)")) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.BYTE_TYPE);
                }
                break;
            case MYSQL_TINYINT_UNSIGNED:
            case MYSQL_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case MYSQL_SMALLINT_UNSIGNED:
            case MYSQL_MEDIUMINT:
            case MYSQL_MEDIUMINT_UNSIGNED:
            case MYSQL_INT:
            case MYSQL_INTEGER:
            case MYSQL_YEAR:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case MYSQL_INT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
            case MYSQL_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case MYSQL_BIGINT_UNSIGNED:
                DecimalType intDecimalType = new DecimalType(20, 0);
                builder.dataType(intDecimalType);
                builder.columnLength(Long.valueOf(intDecimalType.getPrecision()));
                builder.scale(intDecimalType.getScale());
                break;
            case MYSQL_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case MYSQL_FLOAT_UNSIGNED:
                log.warn("{} will probably cause value overflow.", MYSQL_FLOAT_UNSIGNED);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case MYSQL_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case MYSQL_DOUBLE_UNSIGNED:
                log.warn("{} will probably cause value overflow.", MYSQL_DOUBLE_UNSIGNED);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case MYSQL_DECIMAL:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);

                DecimalType decimalType;
                if (typeDefine.getPrecision() > DEFAULT_PRECISION) {
                    log.warn("{} will probably cause value overflow.", MYSQL_DECIMAL);
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                } else {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(),
                                    typeDefine.getScale() == null
                                            ? 0
                                            : typeDefine.getScale().intValue());
                }
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;
            case MYSQL_DECIMAL_UNSIGNED:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);

                log.warn("{} will probably cause value overflow.", MYSQL_DECIMAL_UNSIGNED);
                DecimalType decimalUnsignedType =
                        new DecimalType(
                                typeDefine.getPrecision().intValue() + 1,
                                typeDefine.getScale() == null
                                        ? 0
                                        : typeDefine.getScale().intValue());
                builder.dataType(decimalUnsignedType);
                builder.columnLength(Long.valueOf(decimalUnsignedType.getPrecision()));
                builder.scale(decimalUnsignedType.getScale());
                break;
            case MYSQL_ENUM:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(100L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case MYSQL_CHAR:
            case MYSQL_VARCHAR:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case MYSQL_TINYTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_8 - 1);
                break;
            case MYSQL_TEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_16 - 1);
                break;
            case MYSQL_MEDIUMTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_24 - 1);
                break;
            case MYSQL_LONGTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_32 - 1);
                break;
            case MYSQL_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case MYSQL_BINARY:
            case MYSQL_VARBINARY:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(1L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case MYSQL_TINYBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_8 - 1);
                break;
            case MYSQL_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_16 - 1);
                break;
            case MYSQL_MEDIUMBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_24 - 1);
                break;
            case MYSQL_LONGBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_32 - 1);
                break;
            case MYSQL_GEOMETRY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case MYSQL_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case MYSQL_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case VECTOR_TYPE_NAME:
                String columnType = typeDefine.getColumnType();
                if (columnType.startsWith("vector(") && columnType.endsWith(")")) {
                    Integer number =
                            Integer.parseInt(
                                    columnType.substring(
                                            columnType.indexOf("(") + 1, columnType.indexOf(")")));
                    builder.dataType(VectorType.VECTOR_FLOAT_TYPE);
                    builder.scale(number);
                }
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.OCENABASE, mysqlDataType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<OceanBaseMysqlType> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.<OceanBaseMysqlType>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.nativeType(OceanBaseMysqlType.NULL);
                builder.columnType(MYSQL_NULL);
                builder.dataType(MYSQL_NULL);
                break;
            case BOOLEAN:
                builder.nativeType(OceanBaseMysqlType.BOOLEAN);
                builder.columnType(String.format("%s(%s)", MYSQL_TINYINT, 1));
                builder.dataType(MYSQL_TINYINT);
                builder.length(1L);
                break;
            case TINYINT:
                builder.nativeType(OceanBaseMysqlType.TINYINT);
                builder.columnType(MYSQL_TINYINT);
                builder.dataType(MYSQL_TINYINT);
                break;
            case SMALLINT:
                builder.nativeType(OceanBaseMysqlType.SMALLINT);
                builder.columnType(MYSQL_SMALLINT);
                builder.dataType(MYSQL_SMALLINT);
                break;
            case INT:
                builder.nativeType(OceanBaseMysqlType.INT);
                builder.columnType(MYSQL_INT);
                builder.dataType(MYSQL_INT);
                break;
            case BIGINT:
                builder.nativeType(OceanBaseMysqlType.BIGINT);
                builder.columnType(MYSQL_BIGINT);
                builder.dataType(MYSQL_BIGINT);
                break;
            case FLOAT:
                builder.nativeType(OceanBaseMysqlType.FLOAT);
                builder.columnType(MYSQL_FLOAT);
                builder.dataType(MYSQL_FLOAT);
                break;
            case DOUBLE:
                builder.nativeType(OceanBaseMysqlType.DOUBLE);
                builder.columnType(MYSQL_DOUBLE);
                builder.dataType(MYSQL_DOUBLE);
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

                builder.nativeType(OceanBaseMysqlType.DECIMAL);
                builder.columnType(String.format("%s(%s,%s)", MYSQL_DECIMAL, precision, scale));
                builder.dataType(MYSQL_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.nativeType(OceanBaseMysqlType.VARBINARY);
                    builder.columnType(
                            String.format("%s(%s)", MYSQL_VARBINARY, MAX_VARBINARY_LENGTH / 2));
                    builder.dataType(MYSQL_VARBINARY);
                } else if (column.getColumnLength() < MAX_VARBINARY_LENGTH) {
                    builder.nativeType(OceanBaseMysqlType.VARBINARY);
                    builder.columnType(
                            String.format("%s(%s)", MYSQL_VARBINARY, column.getColumnLength()));
                    builder.dataType(MYSQL_VARBINARY);
                } else if (column.getColumnLength() < POWER_2_24) {
                    builder.nativeType(OceanBaseMysqlType.MEDIUMBLOB);
                    builder.columnType(MYSQL_MEDIUMBLOB);
                    builder.dataType(MYSQL_MEDIUMBLOB);
                } else {
                    builder.nativeType(OceanBaseMysqlType.LONGBLOB);
                    builder.columnType(MYSQL_LONGBLOB);
                    builder.dataType(MYSQL_LONGBLOB);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.nativeType(OceanBaseMysqlType.LONGTEXT);
                    builder.columnType(MYSQL_LONGTEXT);
                    builder.dataType(MYSQL_LONGTEXT);
                } else if (column.getColumnLength() < POWER_2_8) {
                    builder.nativeType(OceanBaseMysqlType.VARCHAR);
                    builder.columnType(
                            String.format("%s(%s)", MYSQL_VARCHAR, column.getColumnLength()));
                    builder.dataType(MYSQL_VARCHAR);
                } else if (column.getColumnLength() < POWER_2_16) {
                    builder.nativeType(OceanBaseMysqlType.TEXT);
                    builder.columnType(MYSQL_TEXT);
                    builder.dataType(MYSQL_TEXT);
                } else if (column.getColumnLength() < POWER_2_24) {
                    builder.nativeType(OceanBaseMysqlType.MEDIUMTEXT);
                    builder.columnType(MYSQL_MEDIUMTEXT);
                    builder.dataType(MYSQL_MEDIUMTEXT);
                } else {
                    builder.nativeType(OceanBaseMysqlType.LONGTEXT);
                    builder.columnType(MYSQL_LONGTEXT);
                    builder.dataType(MYSQL_LONGTEXT);
                }
                break;
            case DATE:
                builder.nativeType(OceanBaseMysqlType.DATE);
                builder.columnType(MYSQL_DATE);
                builder.dataType(MYSQL_DATE);
                break;
            case TIME:
                builder.nativeType(OceanBaseMysqlType.TIME);
                builder.dataType(MYSQL_TIME);
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
                    builder.columnType(String.format("%s(%s)", MYSQL_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(MYSQL_TIME);
                }
                break;
            case TIMESTAMP:
                builder.nativeType(OceanBaseMysqlType.DATETIME);
                builder.dataType(MYSQL_DATETIME);
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
                    builder.columnType(String.format("%s(%s)", MYSQL_DATETIME, timestampScale));
                    builder.scale(timestampScale);
                } else {
                    builder.columnType(MYSQL_DATETIME);
                }
                break;
            case FLOAT_VECTOR:
                builder.nativeType(VECTOR_NAME);
                builder.columnType(String.format("%s(%s)", VECTOR_NAME, column.getScale()));
                builder.dataType(VECTOR_NAME);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.OCENABASE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }

        return builder.build();
    }
}

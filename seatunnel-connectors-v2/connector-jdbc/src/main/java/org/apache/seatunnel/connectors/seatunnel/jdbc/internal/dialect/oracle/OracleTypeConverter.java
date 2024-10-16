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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

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

// reference https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html
@Slf4j
@AutoService(TypeConverter.class)
public class OracleTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    // -------------------------number----------------------------
    public static final String ORACLE_BINARY_DOUBLE = "BINARY_DOUBLE";
    public static final String ORACLE_BINARY_FLOAT = "BINARY_FLOAT";
    public static final String ORACLE_NUMBER = "NUMBER";
    public static final String ORACLE_FLOAT = "FLOAT";
    public static final String ORACLE_REAL = "REAL";
    public static final String ORACLE_INTEGER = "INTEGER";

    // -------------------------string----------------------------
    public static final String ORACLE_CHAR = "CHAR";
    public static final String ORACLE_NCHAR = "NCHAR";
    public static final String ORACLE_VARCHAR = "VARCHAR";
    public static final String ORACLE_VARCHAR2 = "VARCHAR2";
    public static final String ORACLE_NVARCHAR2 = "NVARCHAR2";
    public static final String ORACLE_LONG = "LONG";
    public static final String ORACLE_ROWID = "ROWID";
    public static final String ORACLE_CLOB = "CLOB";
    public static final String ORACLE_NCLOB = "NCLOB";
    public static final String ORACLE_XML = "XMLTYPE";
    public static final String ORACLE_SYS_XML = "SYS.XMLTYPE";

    // ------------------------------time-------------------------
    public static final String ORACLE_DATE = "DATE";
    public static final String ORACLE_TIMESTAMP = "TIMESTAMP";
    public static final String ORACLE_TIMESTAMP_WITH_TIME_ZONE =
            ORACLE_TIMESTAMP + " WITH TIME ZONE";
    public static final String ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE =
            ORACLE_TIMESTAMP + " WITH LOCAL TIME ZONE";

    // ------------------------------blob-------------------------
    public static final String ORACLE_BLOB = "BLOB";
    public static final String ORACLE_RAW = "RAW";
    public static final String ORACLE_LONG_RAW = "LONG RAW";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = 127;
    public static final int DEFAULT_SCALE = 18;
    public static final int TIMESTAMP_DEFAULT_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 9;
    public static final long MAX_RAW_LENGTH = 2000;
    public static final long MAX_ROWID_LENGTH = 18;
    public static final long MAX_CHAR_LENGTH = 2000;
    public static final long MAX_VARCHAR_LENGTH = 4000;

    public static final long BYTES_2GB = (long) Math.pow(2, 31);
    public static final long BYTES_4GB = (long) Math.pow(2, 32);
    public static final OracleTypeConverter INSTANCE = new OracleTypeConverter();

    private final boolean decimalTypeNarrowing;

    public OracleTypeConverter() {
        this(true);
    }

    public OracleTypeConverter(boolean decimalTypeNarrowing) {
        this.decimalTypeNarrowing = decimalTypeNarrowing;
    }

    @Override
    public String identifier() {
        return DatabaseIdentifier.ORACLE;
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

        String oracleType = typeDefine.getDataType().toUpperCase();
        switch (oracleType) {
            case ORACLE_INTEGER:
                builder.dataType(new DecimalType(DEFAULT_PRECISION, 0));
                builder.columnLength((long) DEFAULT_PRECISION);
                break;
            case ORACLE_NUMBER:
                Long precision = typeDefine.getPrecision();
                if (precision == null || precision == 0 || precision > DEFAULT_PRECISION) {
                    precision = Long.valueOf(DEFAULT_PRECISION);
                }
                Integer scale = typeDefine.getScale();
                if (scale == null) {
                    scale = 127;
                }

                if (scale <= 0) {
                    int newPrecision = (int) (precision - scale);
                    if (newPrecision <= 18 && decimalTypeNarrowing) {
                        if (newPrecision == 1) {
                            builder.dataType(BasicType.BOOLEAN_TYPE);
                        } else if (newPrecision <= 9) {
                            builder.dataType(BasicType.INT_TYPE);
                        } else {
                            builder.dataType(BasicType.LONG_TYPE);
                        }
                    } else if (newPrecision < 38) {
                        builder.dataType(new DecimalType(newPrecision, 0));
                        builder.columnLength((long) newPrecision);
                    } else {
                        builder.dataType(new DecimalType(DEFAULT_PRECISION, 0));
                        builder.columnLength((long) DEFAULT_PRECISION);
                    }
                } else if (scale <= DEFAULT_SCALE) {
                    builder.dataType(new DecimalType(precision.intValue(), scale));
                    builder.columnLength(precision);
                    builder.scale(scale);
                } else {
                    builder.dataType(new DecimalType(precision.intValue(), DEFAULT_SCALE));
                    builder.columnLength(precision);
                    builder.scale(DEFAULT_SCALE);
                }
                break;
            case ORACLE_FLOAT:
                // The float type will be converted to DecimalType(10, -127),
                // which will lose precision in the spark engine
                DecimalType floatDecimal = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                builder.dataType(floatDecimal);
                builder.columnLength((long) floatDecimal.getPrecision());
                builder.scale(floatDecimal.getScale());
                break;
            case ORACLE_BINARY_FLOAT:
            case ORACLE_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case ORACLE_BINARY_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case ORACLE_CHAR:
            case ORACLE_VARCHAR:
            case ORACLE_VARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case ORACLE_NCHAR:
            case ORACLE_NVARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(
                        TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                break;
            case ORACLE_ROWID:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_ROWID_LENGTH);
                break;
            case ORACLE_XML:
            case ORACLE_SYS_XML:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case ORACLE_LONG:
                builder.dataType(BasicType.STRING_TYPE);
                // The maximum length of the column is 2GB-1
                builder.columnLength(BYTES_2GB - 1);
                break;
            case ORACLE_CLOB:
            case ORACLE_NCLOB:
                builder.dataType(BasicType.STRING_TYPE);
                // The maximum length of the column is 4GB-1
                builder.columnLength(BYTES_4GB - 1);
                break;
            case ORACLE_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                // The maximum length of the column is 4GB-1
                builder.columnLength(BYTES_4GB - 1);
                break;
            case ORACLE_RAW:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() == null || typeDefine.getLength() == 0) {
                    builder.columnLength(MAX_RAW_LENGTH);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case ORACLE_LONG_RAW:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                // The maximum length of the column is 2GB-1
                builder.columnLength(BYTES_2GB - 1);
                break;
            case ORACLE_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case ORACLE_TIMESTAMP:
            case ORACLE_TIMESTAMP_WITH_TIME_ZONE:
            case ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                if (typeDefine.getScale() == null) {
                    builder.scale(TIMESTAMP_DEFAULT_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.ORACLE, oracleType, typeDefine.getName());
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
                builder.columnType(String.format("%s(%s)", ORACLE_NUMBER, 1));
                builder.dataType(ORACLE_NUMBER);
                builder.length(1L);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                builder.columnType(ORACLE_INTEGER);
                builder.dataType(ORACLE_INTEGER);
                break;
            case FLOAT:
                builder.columnType(ORACLE_BINARY_FLOAT);
                builder.dataType(ORACLE_BINARY_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(ORACLE_BINARY_DOUBLE);
                builder.dataType(ORACLE_BINARY_DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", ORACLE_NUMBER, precision, scale));
                builder.dataType(ORACLE_NUMBER);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(ORACLE_BLOB);
                    builder.dataType(ORACLE_BLOB);
                } else if (column.getColumnLength() <= MAX_RAW_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", ORACLE_RAW, column.getColumnLength()));
                    builder.dataType(ORACLE_RAW);
                } else {
                    builder.columnType(ORACLE_BLOB);
                    builder.dataType(ORACLE_BLOB);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(
                            String.format("%s(%s)", ORACLE_VARCHAR2, MAX_VARCHAR_LENGTH));
                    builder.dataType(ORACLE_VARCHAR2);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", ORACLE_VARCHAR2, column.getColumnLength()));
                    builder.dataType(ORACLE_VARCHAR2);
                } else {
                    builder.columnType(ORACLE_CLOB);
                    builder.dataType(ORACLE_CLOB);
                }
                break;
            case DATE:
                builder.columnType(ORACLE_DATE);
                builder.dataType(ORACLE_DATE);
                break;
            case TIMESTAMP:
                if (column.getScale() == null || column.getScale() <= 0) {
                    builder.columnType(ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                } else {
                    int timestampScale = column.getScale();
                    if (column.getScale() > MAX_TIMESTAMP_SCALE) {
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
                            String.format("TIMESTAMP(%s) WITH LOCAL TIME ZONE", timestampScale));
                    builder.scale(timestampScale);
                }
                builder.dataType(ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.ORACLE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}

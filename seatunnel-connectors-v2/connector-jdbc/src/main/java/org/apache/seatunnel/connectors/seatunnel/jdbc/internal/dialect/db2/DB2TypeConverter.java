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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2;

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

// reference https://www.ibm.com/docs/en/db2/11.5?topic=statements-create-table#r0000927__title__52
@Slf4j
@AutoService(TypeConverter.class)
public class DB2TypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    public static final String DB2_BOOLEAN = "BOOLEAN";

    public static final String DB2_SMALLINT = "SMALLINT";
    public static final String DB2_INTEGER = "INTEGER";
    public static final String DB2_INT = "INT";
    public static final String DB2_BIGINT = "BIGINT";
    // exact
    public static final String DB2_DECIMAL = "DECIMAL";
    public static final String DB2_DEC = "DEC";
    public static final String DB2_NUMERIC = "NUMERIC";
    public static final String DB2_NUM = "NUM";
    // float
    public static final String DB2_REAL = "REAL";
    public static final String DB2_DOUBLE = "DOUBLE";
    public static final String DB2_DECFLOAT = "DECFLOAT";
    // string
    public static final String DB2_CHARACTER = "CHARACTER";
    public static final String DB2_CHAR = "CHAR";
    public static final String DB2_CHAR_FOR_BIT_DATA = "CHAR FOR BIT DATA";
    public static final String DB2_VARCHAR = "VARCHAR";
    public static final String DB2_VARCHAR_FOR_BIT_DATA = "VARCHAR FOR BIT DATA";
    public static final String DB2_LONG_VARCHAR = "LONG VARCHAR";
    public static final String DB2_LONG_VARCHAR_FOR_BIT_DATA = "LONG VARCHAR FOR BIT DATA";
    public static final String DB2_CLOB = "CLOB";
    // graphic
    public static final String DB2_GRAPHIC = "GRAPHIC";
    public static final String DB2_VARGRAPHIC = "VARGRAPHIC";
    public static final String DB2_DBCLOB = "DBCLOB";
    // ---------------------------binary---------------------------
    public static final String DB2_BINARY = "BINARY";
    public static final String DB2_VARBINARY = "VARBINARY";
    // ------------------------------time-------------------------
    public static final String DB2_DATE = "DATE";
    public static final String DB2_TIME = "TIME";
    public static final String DB2_TIMESTAMP = "TIMESTAMP";
    // ------------------------------blob-------------------------
    public static final String DB2_BLOB = "BLOB";
    // other
    public static final String DB2_XML = "XML";

    public static final int MAX_TIMESTAMP_SCALE = 12;
    public static final long MAX_CHAR_LENGTH = 255;
    public static final long MAX_VARCHAR_LENGTH = 32672;
    public static final long MAX_CLOB_LENGTH = 2147483647;
    public static final long MAX_BINARY_LENGTH = 255;
    public static final long MAX_VARBINARY_LENGTH = 32672;
    public static final long MAX_BLOB_LENGTH = 2147483647;
    public static final long MAX_PRECISION = 31;
    public static final long DEFAULT_PRECISION = 5;
    public static final int MAX_SCALE = (int) (MAX_PRECISION - 1);
    public static final int DEFAULT_SCALE = 0;

    public static final DB2TypeConverter INSTANCE = new DB2TypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.DB_2;
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

        String db2Type = typeDefine.getDataType().toUpperCase();
        switch (db2Type) {
            case DB2_BOOLEAN:
                builder.sourceType(DB2_BOOLEAN);
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DB2_SMALLINT:
                builder.sourceType(DB2_SMALLINT);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case DB2_INT:
            case DB2_INTEGER:
                builder.sourceType(DB2_INT);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DB2_BIGINT:
                builder.sourceType(DB2_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DB2_REAL:
                builder.sourceType(DB2_REAL);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DB2_DOUBLE:
                builder.sourceType(DB2_DOUBLE);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DB2_DECFLOAT:
                builder.sourceType(DB2_DECFLOAT);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DB2_DECIMAL:
                builder.sourceType(
                        String.format(
                                "%s(%s,%s)",
                                DB2_DECIMAL, typeDefine.getPrecision(), typeDefine.getScale()));
                builder.dataType(
                        new DecimalType(
                                Math.toIntExact(typeDefine.getPrecision()), typeDefine.getScale()));
                builder.columnLength(typeDefine.getPrecision());
                builder.scale(typeDefine.getScale());
                break;
            case DB2_CHARACTER:
            case DB2_CHAR:
                builder.sourceType(String.format("%s(%d)", DB2_CHAR, typeDefine.getLength()));
                // For char/varchar this length is in bytes
                builder.columnLength(typeDefine.getLength());
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_VARCHAR:
                builder.sourceType(String.format("%s(%d)", DB2_VARCHAR, typeDefine.getLength()));
                builder.columnLength(typeDefine.getLength());
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_LONG_VARCHAR:
                builder.sourceType(DB2_LONG_VARCHAR);
                // default length is 32700
                builder.columnLength(typeDefine.getLength());
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_CLOB:
                builder.sourceType(String.format("%s(%d)", DB2_CLOB, typeDefine.getLength()));
                builder.columnLength(typeDefine.getLength());
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_GRAPHIC:
                builder.sourceType(String.format("%s(%d)", DB2_GRAPHIC, typeDefine.getLength()));
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_VARGRAPHIC:
                builder.sourceType(String.format("%s(%d)", DB2_VARGRAPHIC, typeDefine.getLength()));
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_DBCLOB:
                builder.sourceType(String.format("%s(%d)", DB2_DBCLOB, typeDefine.getLength()));
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_XML:
                builder.sourceType(DB2_XML);
                builder.columnLength((long) Integer.MAX_VALUE);
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DB2_BINARY:
                builder.sourceType(String.format("%s(%d)", DB2_BINARY, typeDefine.getLength()));
                builder.columnLength(typeDefine.getLength());
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case DB2_VARBINARY:
                builder.sourceType(String.format("%s(%d)", DB2_VARBINARY, typeDefine.getLength()));
                builder.columnLength(typeDefine.getLength());
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case DB2_BLOB:
                builder.sourceType(String.format("%s(%d)", DB2_BLOB, typeDefine.getLength()));
                builder.columnLength(typeDefine.getLength());
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case DB2_DATE:
                builder.sourceType(DB2_DATE);
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DB2_TIME:
                builder.sourceType(DB2_TIME);
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case DB2_TIMESTAMP:
                builder.sourceType(String.format("%s(%d)", DB2_TIMESTAMP, typeDefine.getScale()));
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.DB_2, db2Type, typeDefine.getName());
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
                builder.columnType(DB2_BOOLEAN);
                builder.dataType(DB2_BOOLEAN);
                break;
            case TINYINT:
            case SMALLINT:
                builder.columnType(DB2_SMALLINT);
                builder.dataType(DB2_SMALLINT);
                break;
            case INT:
                builder.columnType(DB2_INT);
                builder.dataType(DB2_INT);
                break;
            case BIGINT:
                builder.columnType(DB2_BIGINT);
                builder.dataType(DB2_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DB2_REAL);
                builder.dataType(DB2_REAL);
                break;
            case DOUBLE:
                builder.columnType(DB2_DOUBLE);
                builder.dataType(DB2_DOUBLE);
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

                builder.columnType(String.format("%s(%s,%s)", DB2_DECIMAL, precision, scale));
                builder.dataType(DB2_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(
                            String.format("%s(%s)", DB2_VARBINARY, MAX_VARBINARY_LENGTH));
                    builder.dataType(DB2_VARBINARY);
                    builder.length(column.getColumnLength());
                } else if (column.getColumnLength() <= MAX_BINARY_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", DB2_BINARY, column.getColumnLength()));
                    builder.dataType(DB2_BINARY);
                    builder.length(column.getColumnLength());
                } else if (column.getColumnLength() <= MAX_VARBINARY_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", DB2_VARBINARY, column.getColumnLength()));
                    builder.dataType(DB2_VARBINARY);
                    builder.length(column.getColumnLength());
                } else {
                    long length = column.getColumnLength();
                    if (length > MAX_BLOB_LENGTH) {
                        length = MAX_BLOB_LENGTH;
                        log.warn(
                                "The length of blob type {} is out of range, "
                                        + "it will be converted to {}({})",
                                column.getName(),
                                DB2_BLOB,
                                length);
                    }
                    builder.columnType(String.format("%s(%s)", DB2_BLOB, length));
                    builder.dataType(DB2_BLOB);
                    builder.length(length);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s(%s)", DB2_VARCHAR, MAX_VARCHAR_LENGTH));
                    builder.dataType(DB2_VARCHAR);
                    builder.length(column.getColumnLength());
                } else if (column.getColumnLength() <= MAX_CHAR_LENGTH) {
                    builder.columnType(String.format("%s(%s)", DB2_CHAR, column.getColumnLength()));
                    builder.dataType(DB2_CHAR);
                    builder.length(column.getColumnLength());
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", DB2_VARCHAR, column.getColumnLength()));
                    builder.dataType(DB2_VARCHAR);
                    builder.length(column.getColumnLength());
                } else {
                    long length = column.getColumnLength();
                    if (length > MAX_CLOB_LENGTH) {
                        length = MAX_CLOB_LENGTH;
                        log.warn(
                                "The length of clob type {} is out of range, "
                                        + "it will be converted to {}({})",
                                column.getName(),
                                DB2_CLOB,
                                length);
                    }
                    builder.columnType(String.format("%s(%s)", DB2_CLOB, length));
                    builder.dataType(DB2_CLOB);
                    builder.length(length);
                }
                break;
            case DATE:
                builder.columnType(DB2_DATE);
                builder.dataType(DB2_DATE);
                break;
            case TIME:
                builder.columnType(DB2_TIME);
                builder.dataType(DB2_TIME);
                break;
            case TIMESTAMP:
                if (column.getScale() != null && column.getScale() > 0) {
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
                    builder.columnType(String.format("%s(%s)", DB2_TIMESTAMP, timestampScale));
                    builder.scale(timestampScale);
                } else {
                    builder.columnType(DB2_TIMESTAMP);
                }
                builder.dataType(DB2_TIMESTAMP);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DB_2,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}

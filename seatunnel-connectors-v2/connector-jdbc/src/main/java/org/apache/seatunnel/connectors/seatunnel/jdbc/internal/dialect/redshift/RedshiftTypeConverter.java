/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
@Slf4j
@AutoService(TypeConverter.class)
public class RedshiftTypeConverter extends PostgresTypeConverter {
    public static final String REDSHIFT_SMALLINT = "SMALLINT";
    public static final String REDSHIFT_INTEGER = "INTEGER";
    public static final String REDSHIFT_BIGINT = "BIGINT";
    public static final String REDSHIFT_NUMERIC = "NUMERIC";
    public static final String REDSHIFT_REAL = "REAL";
    public static final String REDSHIFT_DOUBLE_PRECISION = "DOUBLE PRECISION";
    public static final String REDSHIFT_BOOLEAN = "BOOLEAN";
    public static final String REDSHIFT_CHARACTER = "CHARACTER";
    public static final String REDSHIFT_CHARACTER_VARYING = "CHARACTER VARYING";
    public static final String REDSHIFT_VARBYTE = "VARBYTE";
    public static final String REDSHIFT_BINARY_VARYING = "BINARY VARYING";
    public static final String REDSHIFT_TIME = "TIME WITHOUT TIME ZONE";
    public static final String REDSHIFT_TIMETZ = "TIME WITH TIME ZONE";
    public static final String REDSHIFT_TIMESTAMP = "TIMESTAMP WITHOUT TIME ZONE";
    public static final String REDSHIFT_TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE";
    public static final String REDSHIFT_HLLSKETCH = "HLLSKETCH";
    public static final String REDSHIFT_SUPER = "SUPER";

    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final int MAX_PRECISION = 38;
    public static final int MAX_SCALE = 37;
    public static final long MAX_SUPER_LENGTH = 16777216;
    public static final long MAX_HLLSKETCH_LENGTH = 24580;
    public static final int MAX_CHARACTER_LENGTH = 4096;
    public static final int MAX_CHARACTER_VARYING_LENGTH = 65535;
    public static final long MAX_BINARY_VARYING_LENGTH = 1024000;

    public static final RedshiftTypeConverter INSTANCE = new RedshiftTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.REDSHIFT;
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

        String dataType = typeDefine.getDataType().toUpperCase();
        switch (dataType) {
            case REDSHIFT_BOOLEAN:
                builder.sourceType(REDSHIFT_BOOLEAN);
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case REDSHIFT_SMALLINT:
                builder.sourceType(REDSHIFT_SMALLINT);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case REDSHIFT_INTEGER:
                builder.sourceType(REDSHIFT_INTEGER);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case REDSHIFT_BIGINT:
                builder.sourceType(REDSHIFT_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case REDSHIFT_REAL:
                builder.sourceType(REDSHIFT_REAL);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case REDSHIFT_DOUBLE_PRECISION:
                builder.sourceType(REDSHIFT_DOUBLE_PRECISION);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case REDSHIFT_NUMERIC:
                Long precision = typeDefine.getPrecision();
                Integer scale = typeDefine.getScale();
                if (precision == null || precision <= 0) {
                    precision = Long.valueOf(DEFAULT_PRECISION);
                    scale = DEFAULT_SCALE;
                } else if (precision > MAX_PRECISION) {
                    scale = scale - (int) (precision - MAX_PRECISION);
                    precision = Long.valueOf(MAX_PRECISION);
                }
                builder.sourceType(String.format("%s(%d,%d)", REDSHIFT_NUMERIC, precision, scale));
                builder.dataType(new DecimalType(Math.toIntExact(precision), scale));
                break;
            case REDSHIFT_CHARACTER:
                Long characterLength = typeDefine.getLength();
                if (characterLength == null || characterLength <= 0) {
                    characterLength = Long.valueOf(MAX_CHARACTER_LENGTH);
                }
                builder.sourceType(String.format("%s(%d)", REDSHIFT_CHARACTER, characterLength));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(characterLength);
                break;
            case REDSHIFT_CHARACTER_VARYING:
                Long characterVaryingLength = typeDefine.getLength();
                if (characterVaryingLength == null || characterVaryingLength <= 0) {
                    characterVaryingLength = Long.valueOf(MAX_CHARACTER_VARYING_LENGTH);
                }
                builder.sourceType(
                        String.format(
                                "%s(%d)", REDSHIFT_CHARACTER_VARYING, characterVaryingLength));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(characterVaryingLength);
                break;
            case REDSHIFT_HLLSKETCH:
                builder.sourceType(REDSHIFT_HLLSKETCH);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_HLLSKETCH_LENGTH);
                break;
            case REDSHIFT_SUPER:
                builder.sourceType(REDSHIFT_SUPER);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_SUPER_LENGTH);
                break;
            case REDSHIFT_VARBYTE:
            case REDSHIFT_BINARY_VARYING:
                builder.sourceType(
                        String.format(
                                "%s(%d)", typeDefine.getDataType(), MAX_BINARY_VARYING_LENGTH));
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(MAX_BINARY_VARYING_LENGTH);
                break;
            case REDSHIFT_TIME:
                builder.sourceType(REDSHIFT_TIME);
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(MAX_TIME_SCALE);
                break;
            case REDSHIFT_TIMETZ:
                builder.sourceType(REDSHIFT_TIMETZ);
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(MAX_TIME_SCALE);
                break;
            case REDSHIFT_TIMESTAMP:
                builder.sourceType(REDSHIFT_TIMESTAMP);
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(MAX_TIMESTAMP_SCALE);
                break;
            case REDSHIFT_TIMESTAMPTZ:
                builder.sourceType(REDSHIFT_TIMESTAMPTZ);
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(MAX_TIMESTAMP_SCALE);
                break;
            default:
                try {
                    return super.convert(typeDefine);
                } catch (SeaTunnelRuntimeException e) {
                    throw CommonError.convertToSeaTunnelTypeError(
                            DatabaseIdentifier.REDSHIFT,
                            typeDefine.getDataType(),
                            typeDefine.getName());
                }
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
                builder.columnType(REDSHIFT_BOOLEAN);
                builder.dataType(REDSHIFT_BOOLEAN);
                break;
            case TINYINT:
            case SMALLINT:
                builder.columnType(REDSHIFT_SMALLINT);
                builder.dataType(REDSHIFT_SMALLINT);
                break;
            case INT:
                builder.columnType(REDSHIFT_INTEGER);
                builder.dataType(REDSHIFT_INTEGER);
                break;
            case BIGINT:
                builder.columnType(REDSHIFT_BIGINT);
                builder.dataType(REDSHIFT_BIGINT);
                break;
            case FLOAT:
                builder.columnType(REDSHIFT_REAL);
                builder.dataType(REDSHIFT_REAL);
                break;
            case DOUBLE:
                builder.columnType(REDSHIFT_DOUBLE_PRECISION);
                builder.dataType(REDSHIFT_DOUBLE_PRECISION);
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
                builder.columnType(String.format("%s(%d,%d)", REDSHIFT_NUMERIC, precision, scale));
                builder.dataType(REDSHIFT_NUMERIC);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(
                            String.format(
                                    "%s(%d)",
                                    REDSHIFT_CHARACTER_VARYING, MAX_CHARACTER_VARYING_LENGTH));
                    builder.dataType(REDSHIFT_CHARACTER_VARYING);
                    builder.length((long) MAX_CHARACTER_VARYING_LENGTH);
                } else if (column.getColumnLength() <= MAX_CHARACTER_VARYING_LENGTH) {
                    builder.columnType(
                            String.format(
                                    "%s(%d)",
                                    REDSHIFT_CHARACTER_VARYING, column.getColumnLength()));
                    builder.dataType(REDSHIFT_CHARACTER_VARYING);
                    builder.length(column.getColumnLength());
                } else {
                    log.warn(
                            "The length of string column {} is {}, which exceeds the maximum length of {}, "
                                    + "the length will be set to {}",
                            column.getName(),
                            column.getColumnLength(),
                            MAX_SUPER_LENGTH,
                            MAX_SUPER_LENGTH);
                    builder.columnType(REDSHIFT_SUPER);
                    builder.dataType(REDSHIFT_SUPER);
                }
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(
                            String.format(
                                    "%s(%d)", REDSHIFT_BINARY_VARYING, MAX_BINARY_VARYING_LENGTH));
                    builder.dataType(REDSHIFT_BINARY_VARYING);
                } else if (column.getColumnLength() <= MAX_BINARY_VARYING_LENGTH) {
                    builder.columnType(
                            String.format(
                                    "%s(%d)", REDSHIFT_BINARY_VARYING, column.getColumnLength()));
                    builder.dataType(REDSHIFT_BINARY_VARYING);
                    builder.length(column.getColumnLength());
                } else {
                    builder.columnType(
                            String.format(
                                    "%s(%d)", REDSHIFT_BINARY_VARYING, MAX_BINARY_VARYING_LENGTH));
                    builder.dataType(REDSHIFT_BINARY_VARYING);
                    log.warn(
                            "The length of binary column {} is {}, which exceeds the maximum length of {}, "
                                    + "the length will be set to {}",
                            column.getName(),
                            column.getColumnLength(),
                            MAX_BINARY_VARYING_LENGTH,
                            MAX_BINARY_VARYING_LENGTH);
                }
                break;
            case TIME:
                Integer timeScale = column.getScale();
                if (timeScale != null && timeScale > MAX_TIME_SCALE) {
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
                builder.columnType(REDSHIFT_TIME);
                builder.dataType(REDSHIFT_TIME);
                builder.scale(timeScale);
                break;
            case TIMESTAMP:
                Integer timestampScale = column.getScale();
                if (timestampScale != null && timestampScale > MAX_TIMESTAMP_SCALE) {
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
                builder.columnType(REDSHIFT_TIMESTAMP);
                builder.dataType(REDSHIFT_TIMESTAMP);
                builder.scale(timestampScale);
                break;
            case MAP:
            case ARRAY:
            case ROW:
                builder.columnType(REDSHIFT_SUPER);
                builder.dataType(REDSHIFT_SUPER);
                break;
            default:
                try {
                    return super.reconvert(column);
                } catch (SeaTunnelRuntimeException e) {
                    throw CommonError.convertToConnectorTypeError(
                            DatabaseIdentifier.REDSHIFT,
                            column.getDataType().getSqlType().name(),
                            column.getName());
                }
        }
        return builder.build();
    }
}

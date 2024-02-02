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

package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Doris type converter for version 2.x */
@Slf4j
@AutoService(TypeConverter.class)
public class DorisTypeConverterV2 extends AbstractDorisTypeConverter {

    public static final String DORIS_ARRAY = "ARRAY";

    public static final String DORIS_ARRAY_BOOLEAN_INTER = "tinyint(1)";
    public static final String DORIS_ARRAY_TINYINT_INTER = "tinyint(4)";
    public static final String DORIS_ARRAY_SMALLINT_INTER = "smallint(6)";
    public static final String DORIS_ARRAY_INT_INTER = "int(11)";
    public static final String DORIS_ARRAY_BIGINT_INTER = "bigint(20)";

    public static final String DORIS_MAP = "MAP";

    public static final DorisTypeConverterV2 INSTANCE = new DorisTypeConverterV2();

    @Override
    public String identifier() {
        return DorisConfig.IDENTIFIER;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder = getPhysicalColumnBuilder(typeDefine);
        String dorisColumnType = getDorisColumnName(typeDefine);

        switch (dorisColumnType) {
            case DORIS_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DORIS_DATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case DORIS_DECIMALV3:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);
                DecimalType decimalType;
                decimalType =
                        new DecimalType(
                                typeDefine.getPrecision().intValue(),
                                typeDefine.getScale() == null
                                        ? 0
                                        : typeDefine.getScale().intValue());
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;
            case DORIS_ARRAY:
                convertArray(typeDefine.getColumnType(), builder, typeDefine.getName());
                break;
            default:
                super.sampleTypeConverter(builder, typeDefine, dorisColumnType);
        }

        return builder.build();
    }

    private void convertArray(
            String columnType, PhysicalColumn.PhysicalColumnBuilder builder, String name) {
        String columnInterType = extractArrayType(columnType);
        if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_BOOLEAN_INTER)) {
            builder.dataType(ArrayType.BOOLEAN_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_TINYINT_INTER)) {
            builder.dataType(ArrayType.BYTE_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_SMALLINT_INTER)) {
            builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_INT_INTER)) {
            builder.dataType(ArrayType.INT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_BIGINT_INTER)) {
            builder.dataType(ArrayType.LONG_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_FLOAT)) {
            builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_DOUBLE)) {
            builder.dataType(ArrayType.DOUBLE_ARRAY_TYPE);
        } else if (columnInterType.toUpperCase(Locale.ROOT).startsWith("CHAR")
                || columnInterType.toUpperCase(Locale.ROOT).startsWith("VARCHAR")
                || columnInterType.equalsIgnoreCase(DORIS_LARGEINT)
                || columnInterType.equalsIgnoreCase(DORIS_STRING)) {
            builder.dataType(ArrayType.STRING_ARRAY_TYPE);
        } else {
            throw CommonError.convertToSeaTunnelTypeError(DorisConfig.IDENTIFIER, columnType, name);
        }
    }

    private static String extractArrayType(String input) {
        Pattern pattern = Pattern.compile("<(.*?)>");
        Matcher matcher = pattern.matcher(input);

        return matcher.find() ? matcher.group(1) : "";
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder = getBasicTypeDefineBuilder(column);

        switch (column.getDataType().getSqlType()) {
            case STRING:
                reconvertString(column, builder);
                break;
            case DATE:
                builder.columnType(DORIS_DATE);
                builder.dataType(DORIS_DATE);
                break;
            case TIMESTAMP:
                if (column.getScale() != null
                        && column.getScale() > 0
                        && column.getScale() <= MAX_DATETIME_SCALE) {
                    builder.columnType(String.format("%s(%s)", DORIS_DATETIME, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(String.format("%s(%s)", DORIS_DATETIME, MAX_DATETIME_SCALE));
                    builder.scale(MAX_DATETIME_SCALE);
                }
                builder.dataType(DORIS_DATETIME);
                break;
            default:
                super.sampleReconvert(column, builder);
        }
        return builder.build();
    }

    private void reconvertString(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        // source is doris too.
        if (column.getSourceType() != null && column.getSourceType().equalsIgnoreCase(DORIS_JSON)) {
            // Compatible with Doris 1.x and Doris 2.x versions
            builder.columnType(DORIS_JSON);
            builder.dataType(DORIS_JSON);
            return;
        }

        super.sampleReconvertString(column, builder);
    }
}

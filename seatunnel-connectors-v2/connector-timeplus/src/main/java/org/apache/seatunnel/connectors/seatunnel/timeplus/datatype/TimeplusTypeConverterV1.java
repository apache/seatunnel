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

package org.apache.seatunnel.connectors.seatunnel.timeplus.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

/** Timeplus type converter for version 1.2.x */
@Slf4j
@AutoService(TypeConverter.class)
public class TimeplusTypeConverterV1 extends AbstractTimeplusTypeConverter {

    public static final String TIMEPLUS_DATE = "date32";
    public static final String TIMEPLUS_DATETIME = "datetime64";
    public static final String TIMEPLUS_DATE_ARRAY = "array(date32)";
    public static final String TIMEPLUS_DATETIME_ARRAY = "array(datetime32)";

    public static final TimeplusTypeConverterV1 INSTANCE = new TimeplusTypeConverterV1();

    @Override
    public String identifier() {
        return "Timeplus";
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder = getPhysicalColumnBuilder(typeDefine);
        String timeplusColumnType = getTimeplusColumnName(typeDefine);

        switch (timeplusColumnType) {
            case TIMEPLUS_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case TIMEPLUS_DATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale() == null ? 0 : typeDefine.getScale());
                break;
            case TIMEPLUS_DECIMAL:
                Long p = MAX_PRECISION;
                int scale = MAX_SCALE;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    p = typeDefine.getPrecision();
                }

                if (typeDefine.getScale() != null && typeDefine.getScale() > 0) {
                    scale = typeDefine.getScale();
                }
                DecimalType decimalType;
                decimalType = new DecimalType(p.intValue(), scale);
                builder.dataType(decimalType);
                builder.columnLength(p);
                builder.scale(scale);
                break;
            default:
                super.sampleTypeConverter(builder, typeDefine, timeplusColumnType);
        }

        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder = getBasicTypeDefineBuilder(column);

        switch (column.getDataType().getSqlType()) {
            case STRING:
                reconvertString(column, builder);
                break;
            case DATE:
                builder.columnType(TIMEPLUS_DATE);
                builder.dataType(TIMEPLUS_DATE);
                break;
            case TIMESTAMP:
                if (column.getScale() != null
                        && column.getScale() > 0
                        && column.getScale() <= MAX_DATETIME_SCALE) {
                    builder.columnType(
                            String.format("%s(%s)", TIMEPLUS_DATETIME, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(
                            String.format("%s(%s)", TIMEPLUS_DATETIME, MAX_DATETIME_SCALE));
                    builder.scale(MAX_DATETIME_SCALE);
                }
                builder.dataType(TIMEPLUS_DATETIME);
                break;
            case MAP:
                builder.columnType(TIMEPLUS_MAP);
                builder.dataType(TIMEPLUS_MAP);
                break;
            default:
                super.sampleReconvert(column, builder);
        }
        return builder.build();
    }

    private void reconvertString(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        if (column.getSourceType() != null
                && column.getSourceType().equalsIgnoreCase(TIMEPLUS_JSON)) {
            builder.columnType(TIMEPLUS_JSONB);
            builder.dataType(TIMEPLUS_JSON);
            return;
        }

        super.sampleReconvertString(column, builder);
    }
}

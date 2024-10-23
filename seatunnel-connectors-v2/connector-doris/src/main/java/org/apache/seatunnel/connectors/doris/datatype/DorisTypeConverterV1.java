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
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.IDENTIFIER;

/** Doris type converter for version 1.2.x */
@Slf4j
@AutoService(TypeConverter.class)
public class DorisTypeConverterV1 extends AbstractDorisTypeConverter {

    public static final String DORIS_DATEV2 = "DATEV2";
    public static final String DORIS_DATETIMEV2 = "DATETIMEV2";
    public static final String DORIS_DATEV2_ARRAY = "ARRAY<DATEV2>";
    public static final String DORIS_DATETIMEV2_ARRAY = "ARRAY<DATETIMEV2>";

    public static final DorisTypeConverterV1 INSTANCE = new DorisTypeConverterV1();

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder = getPhysicalColumnBuilder(typeDefine);
        String dorisColumnType = getDorisColumnName(typeDefine);

        switch (dorisColumnType) {
            case DORIS_DATE:
            case DORIS_DATEV2:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DORIS_DATETIME:
            case DORIS_DATETIMEV2:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale() == null ? 0 : typeDefine.getScale());
                break;
            case DORIS_DECIMAL:
            case DORIS_DECIMALV3:
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
                super.sampleTypeConverter(builder, typeDefine, dorisColumnType);
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
                builder.columnType(DORIS_DATEV2);
                builder.dataType(DORIS_DATEV2);
                break;
            case TIMESTAMP:
                if (column.getScale() != null
                        && column.getScale() > 0
                        && column.getScale() <= MAX_DATETIME_SCALE) {
                    builder.columnType(
                            String.format("%s(%s)", DORIS_DATETIMEV2, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(
                            String.format("%s(%s)", DORIS_DATETIMEV2, MAX_DATETIME_SCALE));
                    builder.scale(MAX_DATETIME_SCALE);
                }
                builder.dataType(DORIS_DATETIMEV2);
                break;
            case MAP:
                // doris 1.x have no map type
                builder.columnType(DORIS_JSON);
                builder.dataType(DORIS_JSON);
                break;
            default:
                super.sampleReconvert(column, builder);
        }
        return builder.build();
    }

    private void reconvertString(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        // source is doris too.
        if (column.getSourceType() != null && column.getSourceType().equalsIgnoreCase(DORIS_JSON)) {
            builder.columnType(DORIS_JSONB);
            builder.dataType(DORIS_JSON);
            return;
        }

        super.sampleReconvertString(column, builder);
    }
}

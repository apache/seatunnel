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

package org.apache.seatunnel.connectors.seatunnel.paimon.data;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimeType;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@AutoService(TypeConverter.class)
public class PaimonTypeMapper implements TypeConverter<DataType> {
    private static final AtomicInteger fieldId = new AtomicInteger(-1);
    public static final PaimonTypeMapper INSTANCE = new PaimonTypeMapper();

    @Override
    public String identifier() {
        return PaimonSink.PLUGIN_NAME;
    }

    @Override
    public Column convert(DataType typeDefine) {
        // todo compelete when need
        return null;
    }

    @Override
    public DataType reconvert(Column column) {
        SeaTunnelDataType<?> seaTunnelDataType = column.getDataType();
        switch (seaTunnelDataType.getSqlType()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case BYTES:
                return DataTypes.BYTES();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case TINYINT:
                return DataTypes.TINYINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) seaTunnelDataType;
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision < org.apache.paimon.types.DecimalType.MIN_PRECISION) {
                    precision = org.apache.paimon.types.DecimalType.DEFAULT_PRECISION;
                    scale = org.apache.paimon.types.DecimalType.DEFAULT_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is precision less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (precision > org.apache.paimon.types.DecimalType.MAX_PRECISION) {
                    scale =
                            (int)
                                    Math.max(
                                            0,
                                            scale
                                                    - (precision
                                                            - org.apache.paimon.types.DecimalType
                                                                    .MAX_PRECISION));
                    precision = org.apache.paimon.types.DecimalType.MAX_PRECISION;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            org.apache.paimon.types.DecimalType.MAX_PRECISION,
                            precision,
                            scale);
                }
                if (scale < org.apache.paimon.types.DecimalType.MIN_SCALE) {
                    scale = org.apache.paimon.types.DecimalType.MIN_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is scale less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (scale > precision) {
                    scale = (int) precision;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            precision,
                            scale);
                }
                return DataTypes.DECIMAL((int) precision, scale);
            case DATE:
                return DataTypes.DATE();
            case TIME:
                Integer timeScale = column.getScale();
                if (timeScale != null && timeScale > TimeType.MAX_PRECISION) {
                    timeScale = TimeType.MAX_PRECISION;
                    log.warn(
                            "The time column {} type time({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to time({})",
                            column.getName(),
                            column.getScale(),
                            TimeType.MAX_PRECISION,
                            timeScale);
                }
                return DataTypes.TIME(timeScale);
            case TIMESTAMP:
                Integer timestampScale = column.getScale();
                if (timestampScale != null
                        && timestampScale > LocalZonedTimestampType.MAX_PRECISION) {
                    timestampScale = LocalZonedTimestampType.MAX_PRECISION;
                    log.warn(
                            "The timestamp column {} type timestamp({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to timestamp({})",
                            column.getName(),
                            column.getScale(),
                            LocalZonedTimestampType.MAX_PRECISION,
                            timestampScale);
                }
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(timestampScale);
            case STRING:
                return DataTypes.STRING();
            case ARRAY:
                ArrayType arrayType = (ArrayType) seaTunnelDataType;
                DataType elementType =
                        reconvert(getPhysicalColumn(column, arrayType.getElementType()));
                return DataTypes.ARRAY(elementType);
            case MAP:
                org.apache.seatunnel.api.table.type.MapType mapType =
                        (org.apache.seatunnel.api.table.type.MapType) seaTunnelDataType;
                return DataTypes.MAP(
                        reconvert(getPhysicalColumn(column, mapType.getKeyType())),
                        reconvert(getPhysicalColumn(column, mapType.getValueType())));
            case ROW:
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) seaTunnelDataType;
                DataField[] dataFields = new DataField[seaTunnelRowType.getTotalFields()];
                for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                    String field = seaTunnelRowType.getFieldName(i);
                    SeaTunnelDataType fieldType = seaTunnelRowType.getFieldType(i);
                    int id = fieldId.incrementAndGet();
                    dataFields[i] =
                            new DataField(
                                    id, field, reconvert(getPhysicalColumn(column, fieldType)));
                }
                return DataTypes.ROW(dataFields);
            default:
                throw CommonError.convertToConnectorTypeError(
                        identifier(), column.getDataType().getSqlType().name(), column.getName());
        }
    }

    private PhysicalColumn getPhysicalColumn(
            Column column, SeaTunnelDataType<?> seaTunnelDataType) {
        return PhysicalColumn.of(
                column.getName(),
                seaTunnelDataType,
                0,
                true,
                column.getDefaultValue(),
                column.getComment());
    }
}

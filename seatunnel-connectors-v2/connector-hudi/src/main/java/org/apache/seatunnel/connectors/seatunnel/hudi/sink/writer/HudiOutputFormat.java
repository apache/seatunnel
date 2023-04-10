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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.Serializable;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.BYTE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.SHORT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.VOID_TYPE;

public class HudiOutputFormat implements Serializable {

    public String convertSchema(SeaTunnelRowType seaTunnelRowType) {

        SchemaBuilder.FieldAssembler<Schema> seaTunnelRowBuilder =
                SchemaBuilder.record("seaTunnelRow").fields();

        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            convertType(
                    seaTunnelRowBuilder,
                    seaTunnelRowType.getFieldType(i),
                    seaTunnelRowType.getFieldName(i));
        }
        return seaTunnelRowBuilder.endRecord().toString();
    }

    private void convertType(
            SchemaBuilder.FieldAssembler<Schema> builder,
            SeaTunnelDataType<?> seaTunnelDataType,
            String fieldName) {
        SchemaBuilder.FieldTypeBuilder<Schema> type = builder.name(fieldName).type();
        if (seaTunnelDataType.equals(STRING_TYPE)) {
            type.stringType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(BOOLEAN_TYPE)) {
            type.booleanType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(BYTE_TYPE)
                || seaTunnelDataType.equals(SHORT_TYPE)
                || seaTunnelDataType.equals(INT_TYPE)) {
            type.intType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(LONG_TYPE)) {
            type.longType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(FLOAT_TYPE)) {
            type.floatType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(DOUBLE_TYPE)) {
            type.doubleType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(VOID_TYPE)) {
            type.nullType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(ArrayType.BOOLEAN_ARRAY_TYPE)) {
            type.array().items().booleanType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(ArrayType.STRING_ARRAY_TYPE)) {
            type.array().items().stringType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(ArrayType.BYTE_ARRAY_TYPE)
                || seaTunnelDataType.equals(ArrayType.SHORT_ARRAY_TYPE)
                || seaTunnelDataType.equals(ArrayType.INT_ARRAY_TYPE)
                || seaTunnelDataType.equals(LocalTimeType.LOCAL_DATE_TYPE)
                || seaTunnelDataType.equals(LocalTimeType.LOCAL_TIME_TYPE)) {
            type.array().items().intType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(ArrayType.LONG_ARRAY_TYPE)
                || seaTunnelDataType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
            type.array().items().longType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(ArrayType.FLOAT_ARRAY_TYPE)) {
            type.array().items().floatType().noDefault();
            return;
        } else if (seaTunnelDataType.equals(ArrayType.DOUBLE_ARRAY_TYPE)) {
            type.array().items().doubleType().noDefault();
            return;
        } else if (seaTunnelDataType.getTypeClass() == Map.class) {
            if (((MapType) seaTunnelDataType).getKeyType() != STRING_TYPE) {
                throw new HudiConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unexpected value: " + seaTunnelDataType);
            }
            if (((MapType) seaTunnelDataType).getValueType().equals(BYTE_TYPE)
                    || ((MapType) seaTunnelDataType).getValueType().equals(SHORT_TYPE)
                    || ((MapType) seaTunnelDataType).getValueType().equals(INT_TYPE)
                    || ((MapType) seaTunnelDataType)
                            .getValueType()
                            .equals(LocalTimeType.LOCAL_DATE_TYPE)
                    || ((MapType) seaTunnelDataType)
                            .getValueType()
                            .equals(LocalTimeType.LOCAL_TIME_TYPE)) {
                type.map().values().intType().noDefault();
                return;
            } else if (((MapType) seaTunnelDataType).getValueType().equals(LONG_TYPE)
                    || ((MapType) seaTunnelDataType)
                            .getValueType()
                            .equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
                type.map().values().longType().noDefault();
                return;
            } else if (((MapType) seaTunnelDataType).getValueType().equals(BOOLEAN_TYPE)) {
                type.map().values().booleanType().noDefault();
                return;
            } else if (((MapType) seaTunnelDataType).getValueType().equals(FLOAT_TYPE)) {
                type.map().values().floatType().noDefault();
                return;
            } else if (((MapType) seaTunnelDataType).getValueType().equals(DOUBLE_TYPE)) {
                type.map().values().doubleType().noDefault();
                return;
            } else if (((MapType) seaTunnelDataType).getValueType().equals(VOID_TYPE)) {
                type.map().values().nullType().noDefault();
                return;
            } else if (((MapType) seaTunnelDataType).getValueType().equals(STRING_TYPE)) {
                type.map().values().stringType().noDefault();
                return;
            }
            throw new HudiConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unexpected value: " + seaTunnelDataType);
        }
        throw new HudiConnectorException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unexpected value: " + seaTunnelDataType);
    }
}

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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.concurrent.atomic.AtomicInteger;

public class PaimonTypeMapper {
    private static final AtomicInteger fieldId = new AtomicInteger(-1);

    public static DataType toPaimonType(SeaTunnelDataType dataType) {
        switch (dataType.getSqlType()) {
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
                DecimalType decimalType = (DecimalType) dataType;
                return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                // converter elementType
                DataType elementType = toPaimonType(arrayType.getElementType());
                return DataTypes.ARRAY(elementType);
            case MAP:
                org.apache.seatunnel.api.table.type.MapType mapType =
                        (org.apache.seatunnel.api.table.type.MapType) dataType;
                DataType keyType = toPaimonType(mapType.getKeyType());
                DataType valueType = toPaimonType(mapType.getValueType());
                return DataTypes.MAP(keyType, valueType);
            case ROW:
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) dataType;
                DataField[] dataFields = new DataField[seaTunnelRowType.getTotalFields()];
                for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                    String field = seaTunnelRowType.getFieldName(i);
                    SeaTunnelDataType fieldType = seaTunnelRowType.getFieldType(i);
                    int id = fieldId.incrementAndGet();
                    dataFields[i] = new DataField(id, field, toPaimonType(fieldType));
                }
                return DataTypes.ROW(dataFields);
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            case STRING:
            default:
                return DataTypes.STRING();
        }
    }
}

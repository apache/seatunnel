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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.NonNull;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

public class IcebergTypeMapper {

    public static SeaTunnelDataType<?> mapping(@NonNull Type icebergType) {
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case INTEGER:
                return BasicType.INT_TYPE;
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case STRING:
                return BasicType.STRING_TYPE;
            case FIXED:
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) icebergType;
                return new DecimalType(decimalType.precision(), decimalType.scale());
            case STRUCT:
                return mappingStructType((Types.StructType) icebergType);
            case LIST:
                return mappingListType((Types.ListType) icebergType);
            case MAP:
                return mappingMapType((Types.MapType) icebergType);
            default:
                throw new UnsupportedOperationException(
                    "Unsupported iceberg data type: " + icebergType.typeId());
        }
    }

    private static SeaTunnelRowType mappingStructType(Types.StructType structType) {
        List<Types.NestedField> fields = structType.fields();
        List<String> fieldNames = new ArrayList<>(fields.size());
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>(fields.size());
        for (Types.NestedField field : fields) {
            fieldNames.add(field.name());
            fieldTypes.add(mapping(field.type()));
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[0]),
            fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private static ArrayType mappingListType(Types.ListType listType) {
        switch (listType.elementType().typeId()) {
            case BOOLEAN:
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case INTEGER:
                return ArrayType.INT_ARRAY_TYPE;
            case LONG:
                return ArrayType.LONG_ARRAY_TYPE;
            case FLOAT:
                return ArrayType.FLOAT_ARRAY_TYPE;
            case DOUBLE:
                return ArrayType.DOUBLE_ARRAY_TYPE;
            case STRING:
                return ArrayType.STRING_ARRAY_TYPE;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported iceberg list element type: " + listType.elementType().typeId());
        }
    }

    private static MapType mappingMapType(Types.MapType mapType) {
        return new MapType(mapping(mapType.keyType()), mapping(mapType.valueType()));
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AutoService(DataTypeConvertor.class)
public class MaxComputeDataTypeConvertor implements DataTypeConvertor<TypeInfo> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        if (connectorDataType.startsWith("MAP")) {
            // MAP<key,value>
            int i = connectorDataType.indexOf(",");
            return new MapType(
                    toSeaTunnelType(field, connectorDataType.substring(4, i)),
                    toSeaTunnelType(
                            field,
                            connectorDataType.substring(i + 1, connectorDataType.length() - 1)));
        }
        if (connectorDataType.startsWith("ARRAY")) {
            // ARRAY<element>
            SeaTunnelDataType<?> seaTunnelType =
                    toSeaTunnelType(
                            field, connectorDataType.substring(6, connectorDataType.length() - 1));
            switch (seaTunnelType.getSqlType()) {
                case STRING:
                    return ArrayType.STRING_ARRAY_TYPE;
                case BOOLEAN:
                    return ArrayType.BOOLEAN_ARRAY_TYPE;
                case BYTES:
                    return ArrayType.BYTE_ARRAY_TYPE;
                case SMALLINT:
                    return ArrayType.SHORT_ARRAY_TYPE;
                case INT:
                    return ArrayType.INT_ARRAY_TYPE;
                case BIGINT:
                    return ArrayType.LONG_ARRAY_TYPE;
                case FLOAT:
                    return ArrayType.FLOAT_ARRAY_TYPE;
                case DOUBLE:
                    return ArrayType.DOUBLE_ARRAY_TYPE;
                default:
                    throw CommonError.convertToSeaTunnelTypeError(
                            MaxcomputeConfig.PLUGIN_NAME, connectorDataType, field);
            }
        }
        if (connectorDataType.startsWith("STRUCT")) {
            // STRUCT<field1:type1,field2:type2...>
            // todo: support struct type
            String substring = connectorDataType.substring(7, connectorDataType.length() - 1);
            String[] entryArray = substring.split(",");
            String[] fieldNames = new String[entryArray.length];
            SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[entryArray.length];
            for (int i = 0; i < entryArray.length; i++) {
                String[] fieldNameAndType = entryArray[i].split(":");
                fieldNames[i] = fieldNameAndType[0];
                fieldTypes[i] = toSeaTunnelType(fieldNameAndType[0], fieldNameAndType[1]);
            }
            return new SeaTunnelRowType(fieldNames, fieldTypes);
        }
        if (connectorDataType.startsWith("DECIMAL")) {
            // DECIMAL(precision,scale)
            if (connectorDataType.contains("(")) {
                String substring = connectorDataType.substring(8, connectorDataType.length() - 1);
                String[] split = substring.split(",");
                return new DecimalType(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
            } else {
                return new DecimalType(54, 18);
            }
        }
        if (connectorDataType.startsWith("CHAR") || connectorDataType.startsWith("VARCHAR")) {
            // CHAR(n) or VARCHAR(n)
            return BasicType.STRING_TYPE;
        }
        switch (connectorDataType) {
            case "TINYINT":
            case "BINARY":
                return BasicType.BYTE_TYPE;
            case "SMALLINT":
                return BasicType.SHORT_TYPE;
            case "INT":
                return BasicType.INT_TYPE;
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "FLOAT":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
                return BasicType.DOUBLE_TYPE;
            case "STRING":
                return BasicType.STRING_TYPE;
            case "DATE":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "TIMESTAMP":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "TIME":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "BOOLEAN":
                return DecimalType.BOOLEAN_TYPE;
            case "NULL":
                return BasicType.VOID_TYPE;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        MaxcomputeConfig.PLUGIN_NAME, connectorDataType, field);
        }
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, TypeInfo connectorDataType, Map<String, Object> dataTypeProperties) {
        switch (connectorDataType.getOdpsType()) {
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) connectorDataType;
                return new MapType(
                        toSeaTunnelType(field, mapTypeInfo.getKeyTypeInfo(), dataTypeProperties),
                        toSeaTunnelType(field, mapTypeInfo.getValueTypeInfo(), dataTypeProperties));
            case ARRAY:
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) connectorDataType;
                switch (arrayTypeInfo.getElementTypeInfo().getOdpsType()) {
                    case BOOLEAN:
                        return ArrayType.BOOLEAN_ARRAY_TYPE;
                    case INT:
                        return ArrayType.INT_ARRAY_TYPE;
                    case BIGINT:
                        return ArrayType.LONG_ARRAY_TYPE;
                    case FLOAT:
                        return ArrayType.FLOAT_ARRAY_TYPE;
                    case DOUBLE:
                        return ArrayType.DOUBLE_ARRAY_TYPE;
                    case STRING:
                        return ArrayType.STRING_ARRAY_TYPE;
                    default:
                        throw CommonError.convertToSeaTunnelTypeError(
                                MaxcomputeConfig.PLUGIN_NAME,
                                connectorDataType.getTypeName(),
                                field);
                }
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) connectorDataType;
                List<TypeInfo> fields = structTypeInfo.getFieldTypeInfos();
                List<String> fieldNames = new ArrayList<>(fields.size());
                List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>(fields.size());
                for (TypeInfo f : fields) {
                    fieldNames.add(f.getTypeName());
                    fieldTypes.add(toSeaTunnelType(f.getTypeName(), f, dataTypeProperties));
                }
                return new SeaTunnelRowType(
                        fieldNames.toArray(new String[0]),
                        fieldTypes.toArray(new SeaTunnelDataType[0]));
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) connectorDataType;
                return new DecimalType(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            case VARCHAR:
            case CHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DATETIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case VOID:
                return BasicType.VOID_TYPE;
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        MaxcomputeConfig.PLUGIN_NAME, connectorDataType.getTypeName(), field);
        }
    }

    @Override
    public TypeInfo toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        switch (seaTunnelDataType.getSqlType()) {
            case MAP:
                MapType mapType = (MapType) seaTunnelDataType;
                return TypeInfoFactory.getMapTypeInfo(
                        toConnectorType(field, mapType.getKeyType(), dataTypeProperties),
                        toConnectorType(field, mapType.getValueType(), dataTypeProperties));
            case ARRAY:
                ArrayType arrayType = (ArrayType) seaTunnelDataType;
                return TypeInfoFactory.getArrayTypeInfo(
                        toConnectorType(field, arrayType.getElementType(), dataTypeProperties));
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) seaTunnelDataType;
                List<String> fieldNames = new ArrayList<>(rowType.getTotalFields());
                List<TypeInfo> fieldTypes = new ArrayList<>(rowType.getTotalFields());
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    fieldNames.add(rowType.getFieldName(i));
                    fieldTypes.add(
                            toConnectorType(field, rowType.getFieldType(i), dataTypeProperties));
                }
                return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypes);
            case TINYINT:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT);
            case SMALLINT:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.SMALLINT);
            case INT:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT);
            case BIGINT:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT);
            case BYTES:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BINARY);
            case FLOAT:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.FLOAT);
            case DOUBLE:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DOUBLE);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) seaTunnelDataType;
                return TypeInfoFactory.getDecimalTypeInfo(
                        decimalType.getPrecision(), decimalType.getScale());
            case STRING:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING);
            case DATE:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE);
            case TIMESTAMP:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP);
            case TIME:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME);
            case BOOLEAN:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN);
            case NULL:
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.VOID);
            default:
                throw CommonError.convertToConnectorTypeError(
                        MaxcomputeConfig.PLUGIN_NAME,
                        seaTunnelDataType.getSqlType().toString(),
                        field);
        }
    }

    @Override
    public String getIdentity() {
        return MaxcomputeConfig.PLUGIN_NAME;
    }
}

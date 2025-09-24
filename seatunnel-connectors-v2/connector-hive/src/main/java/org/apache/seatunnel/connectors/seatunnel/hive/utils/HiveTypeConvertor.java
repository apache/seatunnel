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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;

import java.util.LinkedHashMap;

public class HiveTypeConvertor {

    public static SeaTunnelDataType<?> covertHiveTypeToSeaTunnelType(String name, String hiveType) {
        if (hiveType.contains("varchar")) {
            return BasicType.STRING_TYPE;
        }
        if (hiveType.contains("char")) {
            throw CommonError.convertToSeaTunnelTypeError(
                    HiveConstants.CONNECTOR_NAME, PluginType.SOURCE, hiveType, name);
        }
        if (hiveType.contains("binary")) {
            return PrimitiveByteArrayType.INSTANCE;
        }
        if (hiveType.contains("struct")) {
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            int start = hiveType.indexOf("<");
            int end = hiveType.lastIndexOf(">");
            String[] columns = hiveType.substring(start + 1, end).split(",");
            for (String column : columns) {
                String[] splits = column.split(":");
                fields.put(
                        splits[0], covertHiveTypeToSeaTunnelType(splits[0], splits[1]).toString());
            }
            return SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                    name, JsonUtils.toJsonString(fields));
        }
        return SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(name, hiveType);
    }

    public static String seatunnelToHiveType(SeaTunnelDataType<?> seaTunnelType) {
        switch (seaTunnelType.getSqlType()) {
            case STRING:
                return "string";
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "tinyint";
            case SMALLINT:
                return "smallint";
            case INT:
                return "int";
            case BIGINT:
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DECIMAL:
                if (seaTunnelType instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) seaTunnelType;
                    return String.format(
                            "decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
                }
                return "decimal(38,18)";
            case BYTES:
                return "binary";
            case DATE:
                return "date";
            case TIME:
                return "string";
            case TIMESTAMP:
                return "timestamp";
            case ROW:
                if (seaTunnelType instanceof org.apache.seatunnel.api.table.type.SeaTunnelRowType) {
                    org.apache.seatunnel.api.table.type.SeaTunnelRowType rowType =
                            (org.apache.seatunnel.api.table.type.SeaTunnelRowType) seaTunnelType;
                    StringBuilder sb = new StringBuilder("struct<");
                    String[] fieldNames = rowType.getFieldNames();
                    org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] fieldTypes =
                            rowType.getFieldTypes();
                    for (int i = 0; i < fieldNames.length; i++) {
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append(fieldNames[i])
                                .append(':')
                                .append(seatunnelToHiveType(fieldTypes[i]));
                    }
                    sb.append('>');
                    return sb.toString();
                }
                throw new UnsupportedOperationException("ROW type requires field names");
            case ARRAY:
                if (seaTunnelType instanceof org.apache.seatunnel.api.table.type.ArrayType) {
                    org.apache.seatunnel.api.table.type.ArrayType<?, ?> arrayType =
                            (org.apache.seatunnel.api.table.type.ArrayType<?, ?>) seaTunnelType;
                    return "array<" + seatunnelToHiveType(arrayType.getElementType()) + ">";
                }
                return "array";
            case MAP:
                if (seaTunnelType instanceof org.apache.seatunnel.api.table.type.MapType) {
                    org.apache.seatunnel.api.table.type.MapType<?, ?> mapType =
                            (org.apache.seatunnel.api.table.type.MapType<?, ?>) seaTunnelType;
                    return "map<"
                            + seatunnelToHiveType(mapType.getKeyType())
                            + ","
                            + seatunnelToHiveType(mapType.getValueType())
                            + ">";
                }
                return "map";
            case NULL:
                throw new UnsupportedOperationException("Orc does not support NULL type");
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported type conversion from %s to Hive ORC type",
                                seaTunnelType.getSqlType()));
        }
    }
}

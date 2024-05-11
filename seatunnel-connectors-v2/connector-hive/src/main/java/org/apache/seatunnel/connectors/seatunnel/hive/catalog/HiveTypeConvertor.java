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

package org.apache.seatunnel.connectors.seatunnel.hive.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
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

    public static String columnToHiveType(Column column) {
        return String.format(
                "`%s` %s %s ",
                column.getName(),
                convertSeaTunnelTypeToHiveType(
                        column.getDataType(),
                        column.getColumnLength() == null ? 0 : column.getColumnLength()),
                column.isNullable() ? "" : "NOT NULL");
    }

    public static String convertSeaTunnelTypeToHiveType(
            SeaTunnelDataType<?> dataType, Long columnLength) {
        switch (dataType.getSqlType()) {
            case STRING:
                return "STRING";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return String.format(
                        "DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            case DATE:
                return "DATE";
            case TIME:
            case TIMESTAMP:
                return "TIMESTAMP";
            case ARRAY:
                return "ARRAY<"
                        + convertSeaTunnelTypeToHiveType(
                                ((ArrayType<?, ?>) dataType).getElementType(), Long.MAX_VALUE)
                        + ">";
            case MAP:
                MapType mapType = (MapType) dataType;
                return String.format(
                        "MAP<%s, %s>",
                        convertSeaTunnelTypeToHiveType(mapType.getKeyType(), Long.MAX_VALUE),
                        convertSeaTunnelTypeToHiveType(mapType.getValueType(), Long.MAX_VALUE));
            case ROW:
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) dataType;
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("STRUCT<");
                String[] fieldNames = seaTunnelRowType.getFieldNames();
                for (int i = 0; i < fieldNames.length; i++) {
                    stringBuilder.append(fieldNames[i]);
                    stringBuilder.append(" : ");
                    stringBuilder.append(
                            convertSeaTunnelTypeToHiveType(
                                    seaTunnelRowType.getFieldType(i), Long.MAX_VALUE));
                    stringBuilder.append(",");
                }
                // delete last comma
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                stringBuilder.append(">");
                return stringBuilder.toString();
            case BYTES:
                return "BINARY";
            case NULL:
            default:
                throw new CatalogException(String.format("Unsupported hive type: %s", dataType));
        }
    }
}

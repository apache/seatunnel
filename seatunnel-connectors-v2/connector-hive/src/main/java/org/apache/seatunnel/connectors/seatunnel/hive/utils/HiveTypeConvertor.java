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
            return BasicType.STRING_TYPE;
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
}

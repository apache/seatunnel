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

package org.apache.seatunnel.connectors.seatunnel.lance.data;

import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;

import com.google.auto.service.AutoService;
import com.lancedb.lance.namespace.model.JsonArrowDataType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class LanceTypeMapper {

    public static final LanceTypeMapper INSTANCE = new LanceTypeMapper();

    public SeaTunnelDataType<?> convertDataType(String field, @NonNull JsonArrowDataType type) {

        switch (type.getType().toLowerCase()) {
            case "bool":
                return BasicType.BOOLEAN_TYPE;
            case "int":
                return BasicType.INT_TYPE;
            case "utf8":
            case "largeutf8":
                return BasicType.STRING_TYPE;
            case "decimal":
                return new DecimalType(8, 4);
            case "floatingpoint":
                return BasicType.FLOAT_TYPE;
            case "date":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "time":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "timestamp":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                // TODO: struct|list|map
            default:
                throw CommonError.convertToSeaTunnelTypeError("Lance", type.getType(), field);
        }
    }
}

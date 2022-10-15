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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.util.HashMap;
import java.util.Map;

public class EsTypeMappingSeaTunnelType {

    private static final Map<String, SeaTunnelDataType> MAPPING = new HashMap() {
        {
            put("string", BasicType.STRING_TYPE);
            put("keyword", BasicType.STRING_TYPE);
            put("text", BasicType.STRING_TYPE);
            put("boolean", BasicType.BOOLEAN_TYPE);
            put("byte", BasicType.BYTE_TYPE);
            put("short", BasicType.SHORT_TYPE);
            put("integer", BasicType.INT_TYPE);
            put("long", BasicType.LONG_TYPE);
            put("float", BasicType.FLOAT_TYPE);
            put("half_float", BasicType.FLOAT_TYPE);
            put("double", BasicType.DOUBLE_TYPE);
            put("date", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        }
    };

    public static SeaTunnelDataType getSeaTunnelDataType(String esType) {
        return MAPPING.get(esType);
    }

}

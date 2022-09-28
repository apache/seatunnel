/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.druid.config;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.util.HashMap;
import java.util.Map;

public class DruidTypeMapper {
    public static Map<String, SeaTunnelDataType<?>> DRUID_TYPE_MAPPS = new HashMap<>();

    static {
        // https://druid.apache.org/docs/latest/querying/sql.html#data-types
        DRUID_TYPE_MAPPS.put("CHAR", BasicType.STRING_TYPE);
        DRUID_TYPE_MAPPS.put("VARCHAR", BasicType.STRING_TYPE);
        DRUID_TYPE_MAPPS.put("DECIMAL", BasicType.DOUBLE_TYPE);
        DRUID_TYPE_MAPPS.put("FLOAT", BasicType.FLOAT_TYPE);
        DRUID_TYPE_MAPPS.put("REAL", BasicType.DOUBLE_TYPE);
        DRUID_TYPE_MAPPS.put("DOUBLE", BasicType.DOUBLE_TYPE);
        DRUID_TYPE_MAPPS.put("BOOLEAN", BasicType.LONG_TYPE);
        DRUID_TYPE_MAPPS.put("TINYINT", BasicType.LONG_TYPE);
        DRUID_TYPE_MAPPS.put("SMALLINT", BasicType.LONG_TYPE);
        DRUID_TYPE_MAPPS.put("INTEGER", BasicType.LONG_TYPE);
        DRUID_TYPE_MAPPS.put("BIGINT", BasicType.LONG_TYPE);
        DRUID_TYPE_MAPPS.put("TIMESTAMP", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        DRUID_TYPE_MAPPS.put("DATE", LocalTimeType.LOCAL_DATE_TYPE);
    }
}

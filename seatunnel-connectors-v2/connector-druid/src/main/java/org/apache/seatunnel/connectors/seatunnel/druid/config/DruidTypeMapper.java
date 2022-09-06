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
    public static final  Map<String, SeaTunnelDataType<?>> informationMapping = new HashMap<>();

    static{
            // https://druid.apache.org/docs/latest/querying/sql.html#data-types
            informationMapping.put("CHAR", BasicType.STRING_TYPE);
            informationMapping.put("VARCHAR", BasicType.STRING_TYPE);
            informationMapping.put("DECIMAL", BasicType.DOUBLE_TYPE);
            informationMapping.put("FLOAT", BasicType.FLOAT_TYPE);
            informationMapping.put("REAL", BasicType.DOUBLE_TYPE);
            informationMapping.put("DOUBLE", BasicType.DOUBLE_TYPE);
            informationMapping.put("BOOLEAN", BasicType.LONG_TYPE);
            informationMapping.put("TINYINT", BasicType.LONG_TYPE);
            informationMapping.put("SMALLINT", BasicType.LONG_TYPE);
            informationMapping.put("INTEGER", BasicType.LONG_TYPE);
            informationMapping.put("BIGINT", BasicType.LONG_TYPE);
            informationMapping.put("TIMESTAMP", LocalTimeType.LOCAL_DATE_TIME_TYPE);
            informationMapping.put("DATE", LocalTimeType.LOCAL_DATE_TYPE);
    }
}

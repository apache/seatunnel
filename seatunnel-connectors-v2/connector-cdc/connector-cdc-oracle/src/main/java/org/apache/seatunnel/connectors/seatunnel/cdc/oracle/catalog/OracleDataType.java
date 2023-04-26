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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.catalog;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import oracle.jdbc.OracleType;

import java.util.Map;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class OracleDataType {
    private static final String PRECISION = "precision";
    private static final String SCALE = "scale";

    private final String typeName;
    private final OracleType oracleType;
    private Integer precision;
    private Integer scale;

    public Map<String, Object> getDataTypeProperties() {
        return ImmutableMap.of(PRECISION, precision, SCALE, scale);
    }

    public static Map<String, Object> getDataTypeProperties(Integer precision, Integer scale) {
        return ImmutableMap.of(PRECISION, precision, SCALE, scale);
    }

    public static Integer getPrecision(Map<String, Object> dataTypeProperties) {
        return dataTypeProperties == null ? 0 : (Integer) dataTypeProperties.get(PRECISION);
    }

    public static Integer getScale(Map<String, Object> dataTypeProperties) {
        return dataTypeProperties == null ? 0 : (Integer) dataTypeProperties.get(SCALE);
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.influxdb.converter;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.util.List;

@Slf4j
public class InfluxDBTypeMapper {

    // ============================data types=====================
    private static final String INFLUXDB_STRING = "STRING";
    private static final String INFLUXDB_FLOAT = "FLOAT";
    private static final String INFLUXDB_INTEGER = "INTEGER";
    private static final String INFLUXDB_BOOL = "BOOL";

    public static final String INFLUXDB_BIGINT = "BIGINT";

    private static final String INFLUXDB_DOUBLE = "DOUBLE";

    private static final String INFLUXDB_VARBINARY = "VARBINARY";

    public static SeaTunnelDataType<?> mapping(Pair<List<String>, List<String>> metadata, String fieldName) throws SQLException {
        List<String> fieldNames = metadata.getLeft();
        List<String> fieldTypes = metadata.getRight();
        String influxDBType = fieldTypes.get(fieldNames.indexOf(fieldName)).toUpperCase();
        switch (influxDBType) {
            case INFLUXDB_BOOL:
                return BasicType.BOOLEAN_TYPE;
            case INFLUXDB_INTEGER:
                return BasicType.INT_TYPE;
            case INFLUXDB_FLOAT:
                return BasicType.FLOAT_TYPE;
            case INFLUXDB_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case INFLUXDB_BIGINT:
                return BasicType.LONG_TYPE;
            case INFLUXDB_VARBINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case INFLUXDB_STRING:
            default:
                return BasicType.STRING_TYPE;
        }
    }
}

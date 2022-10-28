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

package org.apache.seatunnel.flink.jdbc.input;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.VOID_TYPE_INFO;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.Map;

public class HiveTypeInformationMap implements TypeInformationMap {

    private static final Map<String, TypeInformation<?>> INFORMATION_MAP = new HashMap<>();

    //The following mapping relationship reference pages
    //https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBCDataTypes
    static {
        //Numeric Data Types
        INFORMATION_MAP.put("tinyint", INT_TYPE_INFO);
        INFORMATION_MAP.put("smallint", SHORT_TYPE_INFO);
        INFORMATION_MAP.put("int", INT_TYPE_INFO);
        INFORMATION_MAP.put("bigint", LONG_TYPE_INFO);
        INFORMATION_MAP.put("float", FLOAT_TYPE_INFO);
        INFORMATION_MAP.put("double", DOUBLE_TYPE_INFO);
        INFORMATION_MAP.put("decimal", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("boolean", BOOLEAN_TYPE_INFO);

        //String Data Types
        INFORMATION_MAP.put("char", STRING_TYPE_INFO);
        INFORMATION_MAP.put("varchar", STRING_TYPE_INFO);
        INFORMATION_MAP.put("binary", STRING_TYPE_INFO);
        INFORMATION_MAP.put("string", STRING_TYPE_INFO);
        INFORMATION_MAP.put("void", VOID_TYPE_INFO);

        //Date and Time Data Types
        INFORMATION_MAP.put("date", SqlTimeTypeInfo.DATE);
        INFORMATION_MAP.put("interval_day_time", SqlTimeTypeInfo.DATE);
        INFORMATION_MAP.put("interval_year_month", SqlTimeTypeInfo.DATE);
        INFORMATION_MAP.put("timestamp", SqlTimeTypeInfo.TIMESTAMP);

        //Complex Data Types
        INFORMATION_MAP.put("map", STRING_TYPE_INFO);
        INFORMATION_MAP.put("array", STRING_TYPE_INFO);
        INFORMATION_MAP.put("struct", STRING_TYPE_INFO);
    }

    @Override
    public TypeInformation<?> getInformation(String datatype) {
        return INFORMATION_MAP.get(datatype);
    }
}

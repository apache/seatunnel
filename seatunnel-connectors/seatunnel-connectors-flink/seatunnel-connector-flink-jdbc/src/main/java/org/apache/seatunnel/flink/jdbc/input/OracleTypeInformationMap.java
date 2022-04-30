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

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

public class OracleTypeInformationMap implements TypeInformationMap {

    private static final Map<String, TypeInformation<?>> INFORMATION_MAP = new HashMap<>();

    static {
        {
            INFORMATION_MAP.put("NVARCHAR2", STRING_TYPE_INFO);
            INFORMATION_MAP.put("VARCHAR2", STRING_TYPE_INFO);
            INFORMATION_MAP.put("FLOAT", DOUBLE_TYPE_INFO);
            INFORMATION_MAP.put("NUMBER", BIG_DEC_TYPE_INFO);
            INFORMATION_MAP.put("LONG", STRING_TYPE_INFO);
            INFORMATION_MAP.put("DATE", SqlTimeTypeInfo.TIMESTAMP);
            INFORMATION_MAP.put("RAW", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
            INFORMATION_MAP.put("LONG RAW", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
            INFORMATION_MAP.put("NCHAR", STRING_TYPE_INFO);
            INFORMATION_MAP.put("CHAR", STRING_TYPE_INFO);
            INFORMATION_MAP.put("BINARY_FLOAT", FLOAT_TYPE_INFO);
            INFORMATION_MAP.put("BINARY_DOUBLE", DOUBLE_TYPE_INFO);
            INFORMATION_MAP.put("ROWID", STRING_TYPE_INFO);
            INFORMATION_MAP.put("NCLOB", STRING_TYPE_INFO);
            INFORMATION_MAP.put("CLOB", STRING_TYPE_INFO);
            INFORMATION_MAP.put("BLOB", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
            INFORMATION_MAP.put("BFILE", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
            INFORMATION_MAP.put("TIMESTAMP", SqlTimeTypeInfo.TIMESTAMP);
            INFORMATION_MAP.put("TIMESTAMP WITH TIME ZONE", SqlTimeTypeInfo.TIMESTAMP);
            INFORMATION_MAP.put("TIMESTAMP WITH LOCAL TIME ZONE", SqlTimeTypeInfo.TIMESTAMP);
        }
    }

    @Override
    public TypeInformation<?> getInformation(String datatype) {
        return INFORMATION_MAP.get(datatype);
    }
}

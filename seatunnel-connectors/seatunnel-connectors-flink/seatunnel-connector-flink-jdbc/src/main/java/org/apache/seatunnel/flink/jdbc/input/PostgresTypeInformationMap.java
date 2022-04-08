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
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.Map;

public class PostgresTypeInformationMap implements TypeInformationMap {

    private static final Map<String, TypeInformation<?>> INFORMATION_MAP = new HashMap<>();

    static {

        {
            INFORMATION_MAP.put("bool", BOOLEAN_TYPE_INFO);
            INFORMATION_MAP.put("bit", BOOLEAN_TYPE_INFO);
            INFORMATION_MAP.put("int8", LONG_TYPE_INFO);
            INFORMATION_MAP.put("bigserial", LONG_TYPE_INFO);
            INFORMATION_MAP.put("oid", LONG_TYPE_INFO);
            INFORMATION_MAP.put("bytea", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
            INFORMATION_MAP.put("char", STRING_TYPE_INFO);
            INFORMATION_MAP.put("bpchar", STRING_TYPE_INFO);
            INFORMATION_MAP.put("numeric", BIG_DEC_TYPE_INFO);
            INFORMATION_MAP.put("int4", INT_TYPE_INFO);
            INFORMATION_MAP.put("serial", INT_TYPE_INFO);
            INFORMATION_MAP.put("int2", SHORT_TYPE_INFO);
            INFORMATION_MAP.put("smallserial", SHORT_TYPE_INFO);
            INFORMATION_MAP.put("float4", FLOAT_TYPE_INFO);
            INFORMATION_MAP.put("float8", DOUBLE_TYPE_INFO);
            INFORMATION_MAP.put("money", DOUBLE_TYPE_INFO);
            INFORMATION_MAP.put("name", STRING_TYPE_INFO);
            INFORMATION_MAP.put("text", STRING_TYPE_INFO);
            INFORMATION_MAP.put("varchar", STRING_TYPE_INFO);
            INFORMATION_MAP.put("date", SqlTimeTypeInfo.DATE);
            INFORMATION_MAP.put("time", SqlTimeTypeInfo.TIME);
            INFORMATION_MAP.put("timez", SqlTimeTypeInfo.TIME);
            INFORMATION_MAP.put("timestamp", SqlTimeTypeInfo.TIMESTAMP);
            INFORMATION_MAP.put("timestamptz", SqlTimeTypeInfo.TIMESTAMP);

        }

    }

    @Override
    public TypeInformation<?> getInformation(String datatype) {
        return INFORMATION_MAP.get(datatype);
    }
}

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

public class MysqlTypeInformationMap implements TypeInformationMap {

    private static final Map<String, TypeInformation<?>> INFORMATION_MAP = new HashMap<>();

    static {

        INFORMATION_MAP.put("VARCHAR", STRING_TYPE_INFO);
        INFORMATION_MAP.put("BOOLEAN", BOOLEAN_TYPE_INFO);
        INFORMATION_MAP.put("TINYINT", INT_TYPE_INFO);
        INFORMATION_MAP.put("TINYINT UNSIGNED", INT_TYPE_INFO);
        INFORMATION_MAP.put("SMALLINT", SHORT_TYPE_INFO);
        INFORMATION_MAP.put("SMALLINT UNSIGNED", INT_TYPE_INFO);
        INFORMATION_MAP.put("INTEGER", INT_TYPE_INFO);
        INFORMATION_MAP.put("INTEGER UNSIGNED", INT_TYPE_INFO);
        INFORMATION_MAP.put("MEDIUMINT", INT_TYPE_INFO);
        INFORMATION_MAP.put("MEDIUMINT UNSIGNED", INT_TYPE_INFO);
        INFORMATION_MAP.put("INT", INT_TYPE_INFO);
        INFORMATION_MAP.put("INT UNSIGNED", LONG_TYPE_INFO);
        INFORMATION_MAP.put("BIGINT", LONG_TYPE_INFO);
        INFORMATION_MAP.put("BIGINT UNSIGNED", STRING_TYPE_INFO);
        INFORMATION_MAP.put("FLOAT", FLOAT_TYPE_INFO);
        INFORMATION_MAP.put("DOUBLE", DOUBLE_TYPE_INFO);
        INFORMATION_MAP.put("CHAR", STRING_TYPE_INFO);
        INFORMATION_MAP.put("TEXT", STRING_TYPE_INFO);
        INFORMATION_MAP.put("LONGTEXT", STRING_TYPE_INFO);
        INFORMATION_MAP.put("DATE", SqlTimeTypeInfo.DATE);
        INFORMATION_MAP.put("TIME", SqlTimeTypeInfo.TIME);
        INFORMATION_MAP.put("DATETIME", SqlTimeTypeInfo.TIMESTAMP);
        INFORMATION_MAP.put("TIMESTAMP", SqlTimeTypeInfo.TIMESTAMP);
        INFORMATION_MAP.put("DECIMAL", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("BINARY", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

    }

    @Override
    public TypeInformation<?> getInformation(String datatype) {
        return INFORMATION_MAP.get(datatype);
    }
}

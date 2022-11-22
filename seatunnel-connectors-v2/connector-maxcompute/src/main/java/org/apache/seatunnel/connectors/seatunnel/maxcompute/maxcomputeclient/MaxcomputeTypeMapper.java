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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.maxcomputeclient;

import com.aliyun.odps.Column;
import org.apache.seatunnel.api.table.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class MaxcomputeTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(MaxcomputeTypeMapper.class);

    // ============================data types=====================

    private static final String MAXCOMPUTE_UNKNOWN = "UNKNOWN";
    private static final String MAXCOMPUTE_BOOLEAN = "BOOLEAN";

    // -------------------------number----------------------------
    private static final String MAXCOMPUTE_TINYINT = "TINYINT";
    private static final String MAXCOMPUTE_SMALLINT = "SMALLINT";
    private static final String MAXCOMPUTE_BIGINT = "BIGINT";

    private static final String MAXCOMPUTE_FLOAT = "FLOAT";

    private static final String MAXCOMPUTE_DOUBLE = "DOUBLE";
    private static final String MAXCOMPUTE_DECIMAL = "DECIMAL32";


    // -------------------------string----------------------------

    private static final String MAXCOMPUTE_VARCHAR = "VARCHAR";
    private static final String MAXCOMPUTE_CHAR = "CHAR";
    private static final String MAXCOMPUTE_STRING = "STRING";


    // ------------------------------time-------------------------

    private static final String MAXCOMPUTE_DATE = "DATE";
    private static final String MAXCOMPUTE_DATETIME = "DATETIME";
    private static final String MAXCOMPUTE_TIMESTAMP = "TIMESTAMP";


    // ------------------------------blob-------------------------

    private static final String MAXCOMPUTE_BINARY = "BINARY";
    private static final int PRECISION = 20;
    public static SeaTunnelDataType<?> mapping(List<Column> columnSchemaList, int colIndex) throws SQLException {
        String maxcomputeType = columnSchemaList.get(colIndex).getTypeInfo().getTypeName().toUpperCase();
        switch (maxcomputeType) {
            case MAXCOMPUTE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case MAXCOMPUTE_TINYINT:
            case MAXCOMPUTE_SMALLINT:
                return BasicType.INT_TYPE;
            case MAXCOMPUTE_BIGINT:
                return BasicType.LONG_TYPE;
            case MAXCOMPUTE_DECIMAL:
                return new DecimalType(PRECISION, 0);
            case MAXCOMPUTE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case MAXCOMPUTE_DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case MAXCOMPUTE_VARCHAR:
            case MAXCOMPUTE_CHAR:
            case MAXCOMPUTE_STRING:
                return BasicType.STRING_TYPE;
            case MAXCOMPUTE_DATE:
            case MAXCOMPUTE_DATETIME:
            case MAXCOMPUTE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case MAXCOMPUTE_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

            //Doesn't support yet

            case MAXCOMPUTE_UNKNOWN:
            default:
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support Maxcompute type '%s' .",
                            maxcomputeType));
        }
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.kudu.ColumnSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class KuduTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(KuduTypeMapper.class);

    // ============================data types=====================

    private static final String KUDU_UNKNOWN = "UNKNOWN";
    private static final String KUDU_BIT = "BOOL";

    // -------------------------number----------------------------
    private static final String KUDU_TINYINT = "INT8";
    private static final String KUDU_MEDIUMINT = "INT32";
    private static final String KUDU_INT = "INT16";
    private static final String KUDU_BIGINT = "INT64";

    private static final String KUDU_FLOAT = "FLOAT";

    private static final String KUDU_DOUBLE = "DOUBLE";
    private static final String KUDU_DECIMAL = "DECIMAL32";


    // -------------------------string----------------------------

    private static final String KUDU_VARCHAR = "STRING";


    // ------------------------------time-------------------------

    private static final String KUDU_UNIXTIME_MICROS = "UNIXTIME_MICROS";


    // ------------------------------blob-------------------------

    private static final String KUDU_BINARY = "BINARY";
    private static final int PRECISION = 20;
    public static SeaTunnelDataType<?> mapping(List<ColumnSchema> columnSchemaList, int colIndex) throws SQLException {
        String kuduType = columnSchemaList.get(colIndex).getType().getName().toUpperCase();
        switch (kuduType) {
            case KUDU_BIT:
                return BasicType.BOOLEAN_TYPE;
            case KUDU_TINYINT:
            case KUDU_MEDIUMINT:
            case KUDU_INT:
                return BasicType.INT_TYPE;
            case KUDU_BIGINT:
                return BasicType.LONG_TYPE;
            case KUDU_DECIMAL:
                return new DecimalType(PRECISION, 0);
            case KUDU_FLOAT:
                return BasicType.FLOAT_TYPE;
            case KUDU_DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case KUDU_VARCHAR:
                return BasicType.STRING_TYPE;
            case KUDU_UNIXTIME_MICROS:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case KUDU_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

            //Doesn't support yet

            case KUDU_UNKNOWN:
            default:
                throw new KuduConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    String.format(
                        "Doesn't support KUDU type '%s' .",
                        kuduType));
        }
    }
}

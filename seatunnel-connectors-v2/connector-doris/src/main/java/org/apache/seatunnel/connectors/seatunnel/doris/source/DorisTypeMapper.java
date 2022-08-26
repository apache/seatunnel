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

package org.apache.seatunnel.connectors.seatunnel.doris.source;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DorisTypeMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DorisTypeMapper.class);

    // ============================data types=====================

    private static final String DORIS_UNKNOWN = "UNKNOWN";

    // -------------------------number----------------------------
    private static final String DORIS_TINYINT = "TINYINT";
    private static final String DORIS_SMALLINT = "SMALLINT";
    private static final String DORIS_INT = "INT";
    private static final String DORIS_BIGINT = "BIGINT";

    private static final String DORIS_FLOAT = "FLOAT";

    private static final String DORIS_DOUBLE = "DOUBLE";
    private static final String DORIS_DECIMAL = "DECIMAL";


    // -------------------------string----------------------------

    private static final String DORIS_STRING = "STRING";
    private static final String DORIS_CHAR = "CHAR";
    private static final String DORIS_VARCHAR = "VARCHAR";


    // ------------------------------time-------------------------

    private static final String DORIS_DATE_TIME = "DATETIME";


    private static final int PRECISION = 20;

    public static SeaTunnelDataType<?> mapping(ResultSetMetaData columnSchemaList, int colIndex) throws SQLException {
        String dorisType = columnSchemaList.getColumnTypeName(colIndex).toUpperCase();
        switch (dorisType) {
            case DORIS_TINYINT:
            case DORIS_SMALLINT:
            case DORIS_INT:
                return BasicType.INT_TYPE;
            case DORIS_BIGINT:
                return BasicType.LONG_TYPE;
            case DORIS_DECIMAL:
                return new DecimalType(PRECISION, 0);
            case DORIS_FLOAT:
                return BasicType.FLOAT_TYPE;
            case DORIS_DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case DORIS_VARCHAR:
            case DORIS_STRING:
            case DORIS_CHAR:
                return BasicType.STRING_TYPE;
            case DORIS_DATE_TIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DORIS_UNKNOWN:
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support DORIS type '%s' .",
                                dorisType));
        }
    }
}

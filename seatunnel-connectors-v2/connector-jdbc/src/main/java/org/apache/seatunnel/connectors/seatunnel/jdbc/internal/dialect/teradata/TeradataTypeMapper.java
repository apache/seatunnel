/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.teradata;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TeradataTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================

    // -------------------------number----------------------------
    private static final String TERADATA_BYTEINT = "BYTEINT";
    private static final String TERADATA_SMALLINT = "SMALLINT";
    private static final String TERADATA_INTEGER = "INTEGER";
    private static final String TERADATA_BIGINT = "BIGINT";
    private static final String TERADATA_FLOAT = "FLOAT";
    private static final String TERADATA_DECIMAL = "DECIMAL";

    // -------------------------string----------------------------
    private static final String TERADATA_CHAR = "CHAR";
    private static final String TERADATA_VARCHAR = "VARCHAR";
    private static final String TERADATA_CLOB = "CLOB";


    // ---------------------------binary---------------------------
    private static final String TERADATA_BYTE = "BYTE";
    private static final String TERADATA_VARBYTE = "VARBYTE";

    // ------------------------------time-------------------------
    private static final String TERADATA_DATE = "DATE";
    private static final String TERADATA_TIME = "TIME";
    private static final String TERADATA_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String TERADATA_BLOB = "BLOB";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String teradataType = metadata.getColumnTypeName(colIndex).toUpperCase();
        switch (teradataType) {
            case TERADATA_BYTEINT:
                return BasicType.BYTE_TYPE;
            case TERADATA_SMALLINT:
                return BasicType.SHORT_TYPE;
            case TERADATA_INTEGER:
                return BasicType.INT_TYPE;
            case TERADATA_BIGINT:
                return BasicType.LONG_TYPE;
            case TERADATA_FLOAT:
                return BasicType.FLOAT_TYPE;
            case TERADATA_DECIMAL:
                return new DecimalType(metadata.getPrecision(colIndex), metadata.getScale(colIndex));
            case TERADATA_CHAR:
            case TERADATA_VARCHAR:
            case TERADATA_CLOB:
                return BasicType.STRING_TYPE;
            case TERADATA_BYTE:
            case TERADATA_VARBYTE:
            case TERADATA_BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            case TERADATA_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TERADATA_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TERADATA_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support TERADATA type '%s' on column '%s'  yet.",
                        teradataType, jdbcColumnName));
        }
    }
}

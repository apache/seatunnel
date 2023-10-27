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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.tablestore;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class TablestoreTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================

    private static final String TABLESTORE_UNKNOWN = "UNKNOWN";

    private static final String TABLESTORE_BOOL = "BOOL";

    // -------------------------number----------------------------
    private static final String TABLESTORE_BIGINT = "BIGINT";
    private static final String TABLESTORE_DOUBLE = "DOUBLE";
    // -------------------------string----------------------------
    private static final String TABLESTORE_VARCHAR = "VARCHAR";
    private static final String TABLESTORE_MEDIUMTEXT = "MEDIUMTEXT";

    // ------------------------------blob-------------------------
    private static final String TABLESTORE_VARBINARY = "VARBINARY";
    private static final String TABLESTORE_MEDIUMBLOB = "MEDIUMBLOB";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String tablestoreServerType = metadata.getColumnTypeName(colIndex).toUpperCase();
        switch (tablestoreServerType) {
            case TABLESTORE_BOOL:
                return BasicType.BOOLEAN_TYPE;
            case TABLESTORE_BIGINT:
                return BasicType.LONG_TYPE;
            case TABLESTORE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case TABLESTORE_VARCHAR:
            case TABLESTORE_MEDIUMTEXT:
                return BasicType.STRING_TYPE;
            case TABLESTORE_VARBINARY:
            case TABLESTORE_MEDIUMBLOB:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case TABLESTORE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support TABLESTORE type '%s' on column '%s'  yet.",
                                tablestoreServerType, jdbcColumnName));
        }
    }
}

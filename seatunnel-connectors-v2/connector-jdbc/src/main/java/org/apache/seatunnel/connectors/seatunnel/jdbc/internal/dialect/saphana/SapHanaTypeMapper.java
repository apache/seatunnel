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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Locale;

public class SapHanaTypeMapper implements JdbcDialectTypeMapper {

    // refer to
    // https://help.sap.com/docs/SAP_BUSINESSOBJECTS_BUSINESS_INTELLIGENCE_PLATFORM/aa4cb9ab429349e49678e146f05d7341/ec3313286fdb101497906a7cb0e91070.html?locale=zh-CN
    private static final String SAP_HANA_BLOB = "blob";
    private static final String SAP_HANA_VARBINARY = "varbinary";
    private static final String SAP_HANA_DATE = "date";
    private static final String SAP_HANA_TIME = "time";
    private static final String SAP_HANA_LONGDATE = "longtime";
    private static final String SAP_HANA_SECONDDATE = "seconddate";
    private static final String SAP_HANA_TIMESTAMP = "timestamp";
    private static final String SAP_HANA_DECIMAL = "decimal";
    private static final String SAP_HANA_REAL = "real";
    private static final String SAP_HANA_SMALLDECIMAL = "smalldecimal";
    private static final String SAP_HANA_BIGINT = "bigint";
    private static final String SAP_HANA_INTEGER = "integer";
    private static final String SAP_HANA_SMALLINT = "smallint";
    private static final String SAP_HANA_TINYINT = "tinyint";
    private static final String SAP_HANA_DOUBLE = "double";
    private static final String SAP_HANA_CLOB = "clob";
    private static final String SAP_HANA_NCLOB = "nclob";
    private static final String SAP_HANA_TEXT = "text";
    private static final String SAP_HANA_ALPHANUM = "alphanum";
    private static final String SAP_HANA_NVARCHAR = "nvarchar";
    private static final String SAP_HANA_SHORTTEXT = "shorttext";
    private static final String SAP_HANA_VARCHAR = "varchar";
    private static final String SAP_HANA_BINARY = "binary";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String typeName = metadata.getColumnTypeName(colIndex).toLowerCase(Locale.ROOT);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (typeName) {
            case SAP_HANA_BLOB:
            case SAP_HANA_VARBINARY:
            case SAP_HANA_BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case SAP_HANA_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case SAP_HANA_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case SAP_HANA_TIMESTAMP:
            case SAP_HANA_LONGDATE:
            case SAP_HANA_SECONDDATE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case SAP_HANA_DECIMAL:
                return new DecimalType(precision, scale);
            case SAP_HANA_REAL:
            case SAP_HANA_SMALLDECIMAL:
                return BasicType.FLOAT_TYPE;
            case SAP_HANA_BIGINT:
                return BasicType.LONG_TYPE;
            case SAP_HANA_INTEGER:
                return BasicType.INT_TYPE;
            case SAP_HANA_SMALLINT:
                return BasicType.SHORT_TYPE;
            case SAP_HANA_TINYINT:
                return BasicType.BYTE_TYPE;
            case SAP_HANA_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case SAP_HANA_CLOB:
            case SAP_HANA_NCLOB:
            case SAP_HANA_TEXT:
            case SAP_HANA_ALPHANUM:
            case SAP_HANA_NVARCHAR:
            case SAP_HANA_SHORTTEXT:
            case SAP_HANA_VARCHAR:
                return BasicType.STRING_TYPE;
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SAP_HANA, typeName, jdbcColumnName);
        }
    }
}

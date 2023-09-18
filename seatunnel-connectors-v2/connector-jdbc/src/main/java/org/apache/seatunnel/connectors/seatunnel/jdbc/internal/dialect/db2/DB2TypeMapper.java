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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DB2TypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // reference https://www.ibm.com/docs/en/ssw_ibm_i_75/pdf/rbafzpdf.pdf
    // ============================data types=====================
    private static final String DB2_BOOLEAN = "BOOLEAN";

    private static final String DB2_ROWID = "ROWID";
    private static final String DB2_SMALLINT = "SMALLINT";
    private static final String DB2_INTEGER = "INTEGER";
    private static final String DB2_INT = "INT";
    private static final String DB2_BIGINT = "BIGINT";
    // exact
    private static final String DB2_DECIMAL = "DECIMAL";
    private static final String DB2_DEC = "DEC";
    private static final String DB2_NUMERIC = "NUMERIC";
    private static final String DB2_NUM = "NUM";
    // float
    private static final String DB2_REAL = "REAL";
    private static final String DB2_FLOAT = "FLOAT";
    private static final String DB2_DOUBLE = "DOUBLE";
    private static final String DB2_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DB2_DECFLOAT = "DECFLOAT";
    // string
    private static final String DB2_CHAR = "CHAR";
    private static final String DB2_VARCHAR = "VARCHAR";
    private static final String DB2_LONG_VARCHAR = "LONG VARCHAR";
    private static final String DB2_CLOB = "CLOB";
    // graphic
    private static final String DB2_GRAPHIC = "GRAPHIC";
    private static final String DB2_VARGRAPHIC = "VARGRAPHIC";
    private static final String DB2_LONG_VARGRAPHIC = "LONG VARGRAPHIC";
    private static final String DB2_DBCLOB = "DBCLOB";

    // ---------------------------binary---------------------------
    private static final String DB2_BINARY = "BINARY";
    private static final String DB2_VARBINARY = "VARBINARY";

    // ------------------------------time-------------------------
    private static final String DB2_DATE = "DATE";
    private static final String DB2_TIME = "TIME";
    private static final String DB2_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String DB2_BLOB = "BLOB";

    // other
    private static final String DB2_XML = "XML";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        switch (columnType) {
            case DB2_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case DB2_SMALLINT:
                return BasicType.SHORT_TYPE;
            case DB2_INT:
            case DB2_INTEGER:
                return BasicType.INT_TYPE;
            case DB2_BIGINT:
                return BasicType.LONG_TYPE;
            case DB2_DECIMAL:
            case DB2_DEC:
            case DB2_NUMERIC:
            case DB2_NUM:
                if (precision > 0) {
                    return new DecimalType(precision, metadata.getScale(colIndex));
                }
                LOG.warn("decimal did define precision,scale, will be Decimal(38,18)");
                return new DecimalType(38, 18);
            case DB2_REAL:
                return BasicType.FLOAT_TYPE;
            case DB2_FLOAT:
            case DB2_DOUBLE:
            case DB2_DOUBLE_PRECISION:
            case DB2_DECFLOAT:
                return BasicType.DOUBLE_TYPE;
            case DB2_CHAR:
            case DB2_VARCHAR:
            case DB2_LONG_VARCHAR:
            case DB2_CLOB:
            case DB2_GRAPHIC:
            case DB2_VARGRAPHIC:
            case DB2_LONG_VARGRAPHIC:
            case DB2_DBCLOB:
                return BasicType.STRING_TYPE;
            case DB2_BINARY:
            case DB2_VARBINARY:
            case DB2_BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            case DB2_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DB2_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DB2_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DB2_ROWID:
                // maybe should support
            case DB2_XML:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support DB2 type '%s' on column '%s'  yet.",
                                columnType, jdbcColumnName));
        }
    }
}

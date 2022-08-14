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


import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
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
    private static final String DB2_DECDOUBLE = "DECDOUBLE";

    // string
    private static final String DB2_CHAR = "CHAR";
    private static final String DB2_VARCHAR = "VARCHAR";
    private static final String DB2_CLOB = "CLOB";
    private static final String DB2_LONGVARCHAR = "LONG VARCHAR";
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
    private static final String DB2_NCHAR_MAPPING = "NCHAR_MAPPING";

    // other
    private static final String DB2_XML = "XML";
    private static final String DB2_LOB = "XML";
    private static final String DB2_DATALINK = "DATALINK";


    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (columnType) {
            case DB2_BOOLEAN:

            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format("Doesn't support DM2 type '%s' on column '%s'  yet.", columnType, jdbcColumnName));
        }
    }
}

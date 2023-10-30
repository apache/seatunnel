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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import java.util.List;

/** Utilities for converting from SqlServer types to SeaTunnel types. */
public class SqlServerTypeUtils {

    // ============================data types=====================

    // -------------------------string----------------------------
    private static final String SQLSERVER_CHAR = "CHAR";
    private static final String SQLSERVER_VARCHAR = "VARCHAR";
    private static final String SQLSERVER_NCHAR = "NCHAR";
    private static final String SQLSERVER_NVARCHAR = "NVARCHAR";
    private static final String SQLSERVER_STRUCT = "STRUCT";
    private static final String SQLSERVER_CLOB = "CLOB";
    private static final String SQLSERVER_LONGVARCHAR = "LONGVARCHAR";
    private static final String SQLSERVER_LONGNVARCHAR = "LONGNVARCHAR";
    private static final String SQLSERVER_TEXT = "TEXT";
    private static final String SQLSERVER_NTEXT = "NTEXT";
    private static final String SQLSERVER_XML = "XML";

    // ------------------------------blob-------------------------
    private static final String SQLSERVER_BLOB = "BLOB";
    private static final String SQLSERVER_VARBINARY = "VARBINARY";

    // ------------------------------number-------------------------
    private static final String SQLSERVER_INTEGER = "INT";
    private static final String SQLSERVER_SMALLINT = "SMALLINT";
    private static final String SQLSERVER_TINYINT = "TINYINT";
    private static final String SQLSERVER_BIGINT = "BIGINT";
    private static final String SQLSERVER_FLOAT = "FLOAT";
    private static final String SQLSERVER_REAL = "REAL";
    private static final String SQLSERVER_DOUBLE = "DOUBLE";
    private static final String SQLSERVER_NUMERIC = "NUMERIC";
    private static final String SQLSERVER_DECIMAL = "DECIMAL";
    private static final String SQLSERVER_SMALLMONEY = "SMALLMONEY";
    private static final String SQLSERVER_MONEY = "MONEY";

    // ------------------------------date-------------------------
    private static final String SQLSERVER_TIMESTAMP = "TIMESTAMP";
    private static final String SQLSERVER_DATE = "DATE";
    private static final String SQLSERVER_TIME = "TIME";
    private static final String SQLSERVER_DATETIMEOFFSET = "DATETIMEOFFSET";
    private static final String SQLSERVER_DATETIME2 = "DATETIME2";
    private static final String SQLSERVER_DATETIME = "DATETIME";
    private static final String SQLSERVER_SMALLDATETIME = "SMALLDATETIME";

    // ------------------------------bool-------------------------
    private static final String SQLSERVER_BIT = "BIT";

    public static SeaTunnelDataType<?> convertFromColumn(Column column) {
        String typeName = column.typeName().toUpperCase();
        switch (typeName) {
            case SQLSERVER_CHAR:
            case SQLSERVER_VARCHAR:
            case SQLSERVER_NCHAR:
            case SQLSERVER_NVARCHAR:
            case SQLSERVER_STRUCT:
            case SQLSERVER_CLOB:
            case SQLSERVER_LONGVARCHAR:
            case SQLSERVER_LONGNVARCHAR:
            case SQLSERVER_TEXT:
            case SQLSERVER_NTEXT:
            case SQLSERVER_XML:
                return BasicType.STRING_TYPE;
            case SQLSERVER_BLOB:
            case SQLSERVER_VARBINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case SQLSERVER_INTEGER:
                return BasicType.INT_TYPE;
            case SQLSERVER_SMALLINT:
            case SQLSERVER_TINYINT:
                return BasicType.SHORT_TYPE;
            case SQLSERVER_BIGINT:
                return BasicType.LONG_TYPE;
            case SQLSERVER_REAL:
                return BasicType.FLOAT_TYPE;
            case SQLSERVER_DOUBLE:
            case SQLSERVER_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case SQLSERVER_NUMERIC:
            case SQLSERVER_DECIMAL:
            case SQLSERVER_SMALLMONEY:
            case SQLSERVER_MONEY:
                return new DecimalType(column.length(), column.scale().orElse(0));
            case SQLSERVER_TIMESTAMP:
            case SQLSERVER_DATETIMEOFFSET:
            case SQLSERVER_DATETIME2:
            case SQLSERVER_DATETIME:
            case SQLSERVER_SMALLDATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case SQLSERVER_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case SQLSERVER_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case SQLSERVER_BIT:
                return BasicType.BOOLEAN_TYPE;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Don't support SqlSever type '%s' yet, jdbcType:'%s'.",
                                column.typeName(), column.jdbcType()));
        }
    }

    public static SeaTunnelRowType convertFromTable(Table table) {

        List<Column> columns = table.columns();
        String[] fieldNames = columns.stream().map(Column::name).toArray(String[]::new);

        SeaTunnelDataType<?>[] fieldTypes =
                columns.stream()
                        .map(SqlServerTypeUtils::convertFromColumn)
                        .toArray(SeaTunnelDataType[]::new);

        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

public class DuckDBTypeMapper implements JdbcDialectTypeMapper {

    /** Uses DuckDB native type names rather than the generic JDBC type-code fallback. */
    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String nativeType = metadata.getColumnTypeName(colIndex);
        String dataType = nativeType.toUpperCase(Locale.ROOT);
        // Check collections first: DECIMAL(p,s)[] is not a scalar DECIMAL.
        if (metadata.getColumnType(colIndex) == Types.ARRAY) {
            dataType = DuckDBTypeConverter.DUCKDB_ARRAY;
        } else if (metadata.getColumnType(colIndex) == Types.STRUCT) {
            dataType = DuckDBTypeConverter.DUCKDB_STRUCT;
        } else if (dataType.startsWith(DuckDBTypeConverter.DUCKDB_DECIMAL + "(")) {
            // The driver supplies scalar precision/scale separately.
            dataType = DuckDBTypeConverter.DUCKDB_DECIMAL;
        } else if (dataType.startsWith(DuckDBTypeConverter.DUCKDB_MAP + "(")) {
            dataType = DuckDBTypeConverter.DUCKDB_MAP;
        }
        return mappingColumn(
                BasicTypeDefine.builder()
                        .name(metadata.getColumnLabel(colIndex))
                        .columnType(nativeType)
                        .dataType(dataType)
                        .sqlType(metadata.getColumnType(colIndex))
                        .nullable(metadata.isNullable(colIndex) != ResultSetMetaData.columnNoNulls)
                        .length((long) metadata.getPrecision(colIndex))
                        .precision((long) metadata.getPrecision(colIndex))
                        .scale(metadata.getScale(colIndex))
                        .build());
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return new DuckDBTypeConverter().convert(typeDefine);
    }
}

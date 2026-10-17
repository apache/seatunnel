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
import java.util.Locale;

public class DuckDBTypeMapper implements JdbcDialectTypeMapper {

    /** Map query metadata without changing the existing table-path converter. */
    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int index) throws SQLException {
        String nativeType = metadata.getColumnTypeName(index);
        String mappedType = nativeType.toUpperCase(Locale.ROOT);
        long precision = metadata.getPrecision(index);
        int scale = metadata.getScale(index);
        // DuckDB reports parameterized native names for decimals, and timestamp aliases use
        // the same LocalDateTime query contract as TIMESTAMP.
        if (mappedType.startsWith("DECIMAL(")) {
            mappedType = DuckDBTypeConverter.DUCKDB_DECIMAL;
        } else {
            switch (mappedType) {
                case "TIMESTAMP_S":
                case "TIMESTAMP_MS":
                case "TIMESTAMP_NS":
                    mappedType = DuckDBTypeConverter.DUCKDB_TIMESTAMP;
                    break;
                case DuckDBTypeConverter.DUCKDB_UTINYINT:
                    mappedType = DuckDBTypeConverter.DUCKDB_SMALLINT;
                    break;
                case DuckDBTypeConverter.DUCKDB_USMALLINT:
                    mappedType = DuckDBTypeConverter.DUCKDB_INTEGER;
                    break;
                case DuckDBTypeConverter.DUCKDB_UINTEGER:
                    mappedType = DuckDBTypeConverter.DUCKDB_BIGINT;
                    break;
                case DuckDBTypeConverter.DUCKDB_UBIGINT:
                    mappedType = DuckDBTypeConverter.DUCKDB_DECIMAL;
                    precision = 20;
                    scale = 0;
                    break;
                case DuckDBTypeConverter.DUCKDB_UHUGEINT:
                    // The full unsigned 128-bit range needs 39 decimal digits, exceeding
                    // SeaTunnel DECIMAL's limit. Preserve it as driver-provided text.
                    mappedType = DuckDBTypeConverter.DUCKDB_STRING;
                    break;
                default:
                    break;
            }
        }
        return mappingColumn(
                BasicTypeDefine.builder()
                        .name(metadata.getColumnLabel(index))
                        .columnType(nativeType)
                        .dataType(mappedType)
                        .sqlType(metadata.getColumnType(index))
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .nullable(metadata.isNullable(index) != ResultSetMetaData.columnNoNulls)
                        .build());
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return new DuckDBTypeConverter().convert(typeDefine);
    }
}

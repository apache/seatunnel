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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Type mapper for DuckDB JDBC operations.
 *
 * <p>Maps DuckDB JDBC metadata to SeaTunnel columns using the DuckDB type converter. Extracts type
 * information from ResultSetMetaData and converts it to SeaTunnel's internal representation.
 */
public class DuckDBTypeMapper implements JdbcDialectTypeMapper {

    /**
     * Map DuckDB type definition to SeaTunnel column.
     *
     * <p>Delegates to DuckDBTypeConverter for actual type conversion.
     *
     * @param typeDefine the DuckDB type definition
     * @return SeaTunnel column with mapped data type
     */
    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return new DuckDBTypeConverter().convert(typeDefine);
    }

    /**
     * Map DuckDB column from ResultSet metadata to SeaTunnel column.
     *
     * <p>Extracts column metadata including name, type, nullability, precision, and scale from JDBC
     * ResultSetMetaData and converts to SeaTunnel column.
     *
     * @param metadata the ResultSet metadata
     * @param colIndex the column index (1-based)
     * @return SeaTunnel column with mapped data type
     * @throws SQLException if metadata extraction fails
     */
    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        long precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();

        return mappingColumn(typeDefine);
    }

    /**
     * Map DuckDB column directly from ResultSet.
     *
     * <p>Convenience method that extracts metadata from ResultSet and delegates to the metadata-
     * based mapping method.
     *
     * @param rs the ResultSet
     * @param colIndex the column index (1-based)
     * @return SeaTunnel column with mapped data type
     * @throws SQLException if metadata extraction fails
     */
    public Column mappingColumn(ResultSet rs, int colIndex) throws SQLException {
        return mappingColumn(rs.getMetaData(), colIndex);
    }
}

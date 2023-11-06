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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;

import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/** Separate the jdbc meta-information type to SeaTunnelDataType into the interface. */
public interface JdbcDialectTypeMapper extends Serializable {

    /** Convert ResultSetMetaData to SeaTunnel data type {@link SeaTunnelDataType}. */
    SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException;

    default Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        SeaTunnelDataType seaTunnelType = mapping(metadata, colIndex);

        String columnName = metadata.getColumnLabel(colIndex);
        int jdbcType = metadata.getColumnType(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);

        int columnLength = precision;
        long longColumnLength = precision;
        long bitLength = 0;
        switch (jdbcType) {
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
                bitLength = precision * 8;
                break;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
            case NCLOB:
                columnLength = precision * 3;
                longColumnLength = precision * 3;
                break;
            default:
                break;
        }

        return PhysicalColumn.of(
                columnName,
                seaTunnelType,
                columnLength,
                isNullable != ResultSetMetaData.columnNoNulls,
                null,
                null,
                nativeType,
                false,
                false,
                bitLength,
                Collections.emptyMap(),
                longColumnLength);
    }
}

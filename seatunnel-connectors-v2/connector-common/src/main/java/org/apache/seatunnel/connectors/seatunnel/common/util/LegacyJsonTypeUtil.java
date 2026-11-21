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

package org.apache.seatunnel.connectors.seatunnel.common.util;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SqlType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Restores the legacy {@code STRING} schema for native JSON source columns.
 *
 * <p>Sources that infer their schema from the database (Doris {@code JSON}/{@code JSONB}, Snowflake
 * {@code VARIANT}) expose those columns as the String-backed {@link SqlType#JSON} logical type by
 * default. Existing pipelines that still rely on the previous {@code STRING} schema can opt out
 * with the connector's {@code handle_json_as_string} option, which routes the inferred table
 * through {@link #mapJsonColumnsToString(CatalogTable)}.
 *
 * <p>Only top-level columns are rewritten: the legacy connector mappings never produced nested JSON
 * element types, so there is no legacy shape to restore inside arrays, maps or rows. Because JSON
 * values are already carried as {@link String}, changing the declared type is enough to get the old
 * reader behavior back without touching the value path.
 */
public final class LegacyJsonTypeUtil {

    private LegacyJsonTypeUtil() {}

    /**
     * Returns a copy of {@code catalogTable} whose top-level {@link SqlType#JSON} columns are
     * declared as {@link BasicType#STRING_TYPE}. Primary key, constraint keys, options, partition
     * keys and comments are preserved. The same instance is returned when no JSON column exists.
     */
    public static CatalogTable mapJsonColumnsToString(CatalogTable catalogTable) {
        TableSchema tableSchema = catalogTable.getTableSchema();
        boolean hasJsonColumn =
                tableSchema.getColumns().stream().anyMatch(LegacyJsonTypeUtil::isJsonColumn);
        if (!hasJsonColumn) {
            return catalogTable;
        }
        List<Column> columns =
                tableSchema.getColumns().stream()
                        .map(
                                column ->
                                        isJsonColumn(column)
                                                ? column.copy(BasicType.STRING_TYPE)
                                                : column)
                        .collect(Collectors.toList());
        TableSchema stringSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .build();
        return CatalogTable.of(
                catalogTable.getTableId(),
                stringSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName(),
                catalogTable.getMetadataSchema());
    }

    private static boolean isJsonColumn(Column column) {
        return column.getDataType() != null && column.getDataType().getSqlType() == SqlType.JSON;
    }
}

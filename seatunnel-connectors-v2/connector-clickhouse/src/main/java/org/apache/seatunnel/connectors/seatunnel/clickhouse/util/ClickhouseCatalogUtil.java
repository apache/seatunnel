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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog.ClickhouseTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.common.util.CatalogUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class ClickhouseCatalogUtil extends CatalogUtil {

    private static final ThreadLocal<Set<String>> PRIMARY_KEY_COLUMNS =
            ThreadLocal.withInitial(HashSet::new);

    public static final ClickhouseCatalogUtil INSTANCE = new ClickhouseCatalogUtil();

    @Override
    public String getCreateTableSql(
            String template,
            String database,
            String table,
            TableSchema tableSchema,
            String comment,
            String optionsKey) {
        Set<String> pkColumns = PRIMARY_KEY_COLUMNS.get();
        pkColumns.clear();
        if (tableSchema.getPrimaryKey() != null) {
            pkColumns.addAll(tableSchema.getPrimaryKey().getColumnNames());
        }
        try {
            return super.getCreateTableSql(
                    template, database, table, tableSchema, comment, optionsKey);
        } finally {
            pkColumns.clear();
        }
    }

    public String columnToConnectorType(Column column) {
        checkNotNull(column, "The column is required.");
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else {
            columnType = ClickhouseTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }

        Set<String> pkColumns = PRIMARY_KEY_COLUMNS.get();
        boolean isPrimaryKeyColumn = pkColumns != null && pkColumns.contains(column.getName());

        if (column.isNullable() && !isUnsupportedNullableType(columnType) && !isPrimaryKeyColumn) {
            columnType = "Nullable(" + columnType + ")";
        }

        return String.format(
                "`%s` %s %s",
                column.getName(),
                columnType,
                StringUtils.isEmpty(column.getComment())
                        ? ""
                        : "COMMENT '"
                                + column.getComment().replace("'", "''").replace("\\", "\\\\")
                                + "'");
    }

    private static boolean isUnsupportedNullableType(String columnType) {
        return columnType.startsWith("Map(") || columnType.startsWith("Array(");
    }

    public String getDropTableSql(TablePath tablePath, boolean ignoreIfNotExists) {
        if (ignoreIfNotExists) {
            return "DROP TABLE IF EXISTS "
                    + tablePath.getDatabaseName()
                    + "."
                    + tablePath.getTableName();
        } else {
            return "DROP TABLE " + tablePath.getDatabaseName() + "." + tablePath.getTableName();
        }
    }

    public String getTruncateTableSql(TablePath tablePath) {
        return "TRUNCATE TABLE " + tablePath.getDatabaseName() + "." + tablePath.getTableName();
    }
}

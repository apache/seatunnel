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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.common.sql.ClauseMergeFormat;
import org.apache.seatunnel.connectors.seatunnel.common.sql.SqlTableClauseMerger;
import org.apache.seatunnel.connectors.seatunnel.common.util.CatalogUtil;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class StarRocksSaveModeUtil extends CatalogUtil {

    public static final StarRocksSaveModeUtil INSTANCE = new StarRocksSaveModeUtil();

    private StarRocksSaveModeUtil() {}

    public String getCreateTableSql(
            String template,
            String database,
            String table,
            TableSchema tableSchema,
            String comment,
            String optionsKey,
            Map<String, String> tableOptions) {
        String createTableSql =
                getCreateTableSql(template, database, table, tableSchema, comment, optionsKey);
        return applyTableOptionsToCreateTableSql(createTableSql, tableOptions);
    }

    public void validateTableOptions(ReadonlyConfig config, Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return;
        }
        if (isCustomCreateTemplate(config)) {
            throw new SeaTunnelRuntimeException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "table_options cannot be used together with a custom save_mode_create_template"
                            + " for StarRocks sink.");
        }
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                throw new SeaTunnelRuntimeException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "table_options contains a blank property key for StarRocks sink.");
            }
            if (entry.getValue() == null) {
                throw new SeaTunnelRuntimeException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "table_options property '%s' has null value for StarRocks sink.",
                                entry.getKey()));
            }
        }
    }

    public String applyTableOptionsToCreateTableSql(
            String createTableSql, Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return createTableSql;
        }
        return SqlTableClauseMerger.merge(
                createTableSql, ClauseMergeFormat.DOUBLE_QUOTED_PROPERTIES, tableOptions);
    }

    private static boolean isCustomCreateTemplate(ReadonlyConfig config) {
        return config.getOptional(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE)
                .map(
                        template ->
                                !template.equals(
                                        StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE
                                                .defaultValue()))
                .orElse(false);
    }

    public String columnToConnectorType(Column column) {
        checkNotNull(column, "The column is required.");
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else {
            columnType =
                    dataTypeToStarrocksType(
                            column.getDataType(),
                            column.getColumnLength() == null ? 0 : column.getColumnLength());
        }
        return String.format(
                "`%s` %s %s %s",
                column.getName(),
                columnType,
                column.isNullable() ? "NULL" : "NOT NULL",
                StringUtils.isEmpty(column.getComment())
                        ? ""
                        : "COMMENT '"
                                + column.getComment().replace("'", "''").replace("\\", "\\\\")
                                + "'");
    }

    private static String dataTypeToStarrocksType(SeaTunnelDataType<?> dataType, long length) {
        checkNotNull(dataType, "The SeaTunnel's data type is required.");
        switch (dataType.getSqlType()) {
            case NULL:
            case TIME:
                return "VARCHAR(8)";
            case STRING:
                if (length > 65533 || length <= 0) {
                    return "STRING";
                } else {
                    return "VARCHAR(" + length + ")";
                }
            case BYTES:
                return "STRING";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "DATETIME";
            case ARRAY:
                return "ARRAY<"
                        + dataTypeToStarrocksType(
                                ((ArrayType<?, ?>) dataType).getElementType(), Long.MAX_VALUE)
                        + ">";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return String.format(
                        "Decimal(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
            case MAP:
            case ROW:
            case JSON:
                return "JSON";
            default:
        }
        throw new IllegalArgumentException("Unsupported SeaTunnel's data type: " + dataType);
    }
}

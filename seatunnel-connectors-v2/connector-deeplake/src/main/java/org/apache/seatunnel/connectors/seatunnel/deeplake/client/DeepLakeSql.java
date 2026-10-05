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

package org.apache.seatunnel.connectors.seatunnel.deeplake.client;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Builds parameterized Deep Lake SQL and validates the supported SeaTunnel type mapping. */
public final class DeepLakeSql {

    private DeepLakeSql() {}

    public static String createTableSql(String workspace, String table, CatalogTable catalogTable) {
        List<Column> schemaColumns = catalogTable.getTableSchema().getColumns();
        List<String> columns = new ArrayList<>(schemaColumns.size() + 1);
        for (Column column : schemaColumns) {
            String definition =
                    quoteIdentifier(column.getName()) + " " + toDeepLakeType(column.getDataType());
            if (!column.isNullable()) {
                definition += " NOT NULL";
            }
            columns.add(definition);
        }

        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            String keys =
                    primaryKey.getColumnNames().stream()
                            .map(DeepLakeSql::quoteIdentifier)
                            .collect(Collectors.joining(", "));
            columns.add("PRIMARY KEY (" + keys + ")");
        }

        return "CREATE TABLE IF NOT EXISTS "
                + qualifiedTable(workspace, table)
                + " ("
                + String.join(", ", columns)
                + ") USING deeplake";
    }

    public static String insertSql(String workspace, String table, SeaTunnelRowType rowType) {
        List<String> columns = new ArrayList<>(rowType.getTotalFields());
        List<String> parameters = new ArrayList<>(rowType.getTotalFields());
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            // Validate every field even when the sink writes to a pre-existing table.
            toDeepLakeType(rowType.getFieldType(i));
            columns.add(quoteIdentifier(rowType.getFieldName(i)));
            String parameter = "$" + (i + 1);
            switch (rowType.getFieldType(i).getSqlType()) {
                case FLOAT_VECTOR:
                    parameter += "::float4[]";
                    break;
                case BINARY_VECTOR:
                case BYTES:
                    parameter = "decode(" + parameter + ", 'base64')";
                    break;
                default:
                    break;
            }
            parameters.add(parameter);
        }
        return "INSERT INTO "
                + qualifiedTable(workspace, table)
                + " ("
                + String.join(", ", columns)
                + ") VALUES ("
                + String.join(", ", parameters)
                + ")";
    }

    static String toDeepLakeType(SeaTunnelDataType<?> type) {
        return toDeepLakeType(type, false);
    }

    private static String toDeepLakeType(SeaTunnelDataType<?> type, boolean arrayElement) {
        switch (type.getSqlType()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return "NUMERIC("
                        + decimalType.getPrecision()
                        + ", "
                        + decimalType.getScale()
                        + ")";
            case STRING:
                return "TEXT";
            case BYTES:
            case BINARY_VECTOR:
                if (arrayElement) {
                    throw new DeepLakeConnectorException(
                            DeepLakeConnectorErrorCode.UNSUPPORTED_DATA_TYPE,
                            "DeepLake sink does not support binary values inside arrays");
                }
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case TIMESTAMP_TZ:
                return "TIMESTAMPTZ";
            case FLOAT_VECTOR:
                return "FLOAT4[]";
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) type;
                return toDeepLakeType(arrayType.getElementType(), true) + "[]";
            default:
                throw new DeepLakeConnectorException(
                        DeepLakeConnectorErrorCode.UNSUPPORTED_DATA_TYPE,
                        "DeepLake sink does not support " + type.getSqlType());
        }
    }

    public static String qualifiedTable(String workspace, String table) {
        return quoteIdentifier(workspace) + "." + quoteIdentifier(table);
    }

    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}

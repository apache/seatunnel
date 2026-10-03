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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class SnowflakeDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return DatabaseIdentifier.SNOWFLAKE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SnowflakeJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SnowflakeTypeMapper();
    }

    /**
     * Wraps JSON parameters with PARSE_JSON so JSON text is stored as a structured VARIANT value.
     */
    @Override
    public String getInsertIntoStatement(
            String database, String tableName, TableSchema tableSchema) {
        String[] fieldNames = tableSchema.getFieldNames();
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames)
                        .map(fieldName -> parameterExpression(tableSchema, fieldName))
                        .collect(Collectors.joining(", "));
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableIdentifier(database, tableName), columns, placeholders);
    }

    /** Wraps JSON parameters with PARSE_JSON in both update values and conditions. */
    @Override
    public String getUpdateStatement(
            String database,
            String tableName,
            TableSchema tableSchema,
            String[] conditionFields,
            boolean isPrimaryKeyUpdated) {
        String setClause =
                Arrays.stream(tableSchema.getFieldNames())
                        .filter(
                                fieldName ->
                                        isPrimaryKeyUpdated
                                                || !Arrays.asList(conditionFields)
                                                        .contains(fieldName))
                        .map(
                                fieldName ->
                                        String.format(
                                                "%s = %s",
                                                quoteIdentifier(fieldName),
                                                parameterExpression(tableSchema, fieldName)))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "%s = %s",
                                                quoteIdentifier(fieldName),
                                                parameterExpression(tableSchema, fieldName)))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "UPDATE %s SET %s WHERE %s",
                tableIdentifier(database, tableName), setClause, conditionClause);
    }

    /** Returns the Snowflake bind expression for a field. */
    private String parameterExpression(TableSchema tableSchema, String fieldName) {
        return tableSchema.getColumn(fieldName).getDataType().getSqlType() == SqlType.JSON
                ? "PARSE_JSON(:" + fieldName + ")"
                : ":" + fieldName;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] pkNames) {
        return Optional.empty();
    }
}

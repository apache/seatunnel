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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class DB2Dialect implements JdbcDialect {

    protected String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public DB2Dialect() {}

    public DB2Dialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DB_2;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DB2JdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DB2TypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\"").append(".");
            }
            return sb.append("\"")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("\"")
                    .toString();
        }
        return "\"" + getFieldIde(identifier, fieldIde) + "\"";
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        // Generate field list for USING and INSERT clauses
        String fieldList =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        // Generate placeholder list for VALUES clause
        String placeholderList =
                Arrays.stream(fieldNames).map(field -> "?").collect(Collectors.joining(", "));

        // Generate ON clause
        String onClause =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                field ->
                                        "target."
                                                + quoteIdentifier(field)
                                                + " = source."
                                                + quoteIdentifier(field))
                        .collect(Collectors.joining(" AND "));

        // Generate WHEN MATCHED clause
        String whenMatchedClause =
                Arrays.stream(fieldNames)
                        .map(
                                field ->
                                        "target."
                                                + quoteIdentifier(field)
                                                + " <> source."
                                                + quoteIdentifier(field))
                        .collect(Collectors.joining(" OR "));

        // Generate UPDATE SET clause
        String updateSetClause =
                Arrays.stream(fieldNames)
                        .map(
                                field ->
                                        "target."
                                                + quoteIdentifier(field)
                                                + " = source."
                                                + quoteIdentifier(field))
                        .collect(Collectors.joining(", "));

        // Generate WHEN NOT MATCHED clause
        String insertClause =
                "INSERT ("
                        + fieldList
                        + ") VALUES ("
                        + Arrays.stream(fieldNames)
                                .map(field -> "source." + quoteIdentifier(field))
                                .collect(Collectors.joining(", "))
                        + ")";

        // Combine all parts to form the final SQL statement
        String mergeStatement =
                String.format(
                        "MERGE INTO %s.%s AS target USING (VALUES (%s)) AS source (%s) ON %s "
                                + "WHEN MATCHED AND (%s) THEN UPDATE SET %s "
                                + "WHEN NOT MATCHED THEN %s",
                        quoteIdentifier(database),
                        quoteIdentifier(tableName),
                        placeholderList,
                        fieldList,
                        onClause,
                        whenMatchedClause,
                        updateSetClause,
                        insertClause);

        return Optional.of(mergeStatement);
    }

    @Override
    public String dualTable() {
        return " FROM SYSIBM.SYSDUMMY1 ";
    }
}

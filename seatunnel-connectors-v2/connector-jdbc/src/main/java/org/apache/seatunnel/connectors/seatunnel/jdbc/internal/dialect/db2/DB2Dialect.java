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

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class DB2Dialect implements JdbcDialect {

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
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        // Generate field list for USING and INSERT clauses
        String fieldList = String.join(", ", fieldNames);

        // Generate placeholder list for VALUES clause
        String placeholderList =
                Arrays.stream(fieldNames).map(field -> "?").collect(Collectors.joining(", "));

        // Generate ON clause
        String onClause =
                Arrays.stream(uniqueKeyFields)
                        .map(field -> "target." + field + " = source." + field)
                        .collect(Collectors.joining(" AND "));

        // Generate WHEN MATCHED clause
        String whenMatchedClause =
                Arrays.stream(fieldNames)
                        .map(field -> "target." + field + " <> source." + field)
                        .collect(Collectors.joining(" OR "));

        // Generate UPDATE SET clause
        String updateSetClause =
                Arrays.stream(fieldNames)
                        .map(field -> "target." + field + " = source." + field)
                        .collect(Collectors.joining(", "));

        // Generate WHEN NOT MATCHED clause
        String insertClause =
                "INSERT ("
                        + fieldList
                        + ") VALUES ("
                        + Arrays.stream(fieldNames)
                                .map(field -> "source." + field)
                                .collect(Collectors.joining(", "))
                        + ")";

        // Combine all parts to form the final SQL statement
        String mergeStatement =
                String.format(
                        "MERGE INTO %s.%s AS target USING (VALUES (%s)) AS source (%s) ON %s "
                                + "WHEN MATCHED AND (%s) THEN UPDATE SET %s "
                                + "WHEN NOT MATCHED THEN %s;",
                        database,
                        tableName,
                        placeholderList,
                        fieldList,
                        onClause,
                        whenMatchedClause,
                        updateSetClause,
                        insertClause);

        return Optional.of(mergeStatement);
    }
}

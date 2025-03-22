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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.opengauss;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class OpenGaussDialect extends PostgresDialect {

    @Override
    public Optional<String> getUpsertStatement(
            String database,
            String tableName,
            String[] fieldNames,
            String[] primaryKeyFields,
            String[] uniqueKeyFields) {
        if (primaryKeyFields == null || primaryKeyFields.length == 0) {
            log.warn("primaryKeyFields is empty, upsert statement will not be generated.");
        }
        if (primaryKeyFields.length == uniqueKeyFields.length) {
            String updateClause =
                    Arrays.stream(fieldNames)
                            .filter(
                                    fieldName ->
                                            !Arrays.asList(primaryKeyFields).contains(fieldName))
                            .map(
                                    fieldName ->
                                            quoteIdentifier(fieldName)
                                                    + "=EXCLUDED."
                                                    + quoteIdentifier(fieldName))
                            .collect(Collectors.joining(", "));
            if (updateClause.isEmpty()) {
                return Optional.empty();
            }
            String upsertSQL =
                    String.format(
                            "%s ON DUPLICATE KEY UPDATE %s",
                            getInsertIntoStatement(database, tableName, fieldNames), updateClause);
            return Optional.of(upsertSQL);
        } else {
            List<String> nonUniqueKeyFields =
                    Arrays.stream(fieldNames)
                            .filter(
                                    fieldName ->
                                            !Arrays.asList(primaryKeyFields).contains(fieldName))
                            .collect(Collectors.toList());
            String valuesBinding =
                    Arrays.stream(fieldNames)
                            .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                            .collect(Collectors.joining(", "));

            String usingClause = String.format(" SELECT %s ", valuesBinding);
            String onConditions =
                    Arrays.stream(primaryKeyFields)
                            .map(
                                    fieldName ->
                                            String.format(
                                                    "TARGET.%s=SOURCE.%s",
                                                    quoteIdentifier(fieldName),
                                                    quoteIdentifier(fieldName)))
                            .collect(Collectors.joining(" AND "));
            String updateSetClause =
                    nonUniqueKeyFields.stream()
                            .map(
                                    fieldName ->
                                            String.format(
                                                    "TARGET.%s=SOURCE.%s",
                                                    quoteIdentifier(fieldName),
                                                    quoteIdentifier(fieldName)))
                            .collect(Collectors.joining(", "));
            String insertFields =
                    Arrays.stream(fieldNames)
                            .map(this::quoteIdentifier)
                            .collect(Collectors.joining(", "));
            String insertValues =
                    Arrays.stream(fieldNames)
                            .map(fieldName -> "SOURCE." + quoteIdentifier(fieldName))
                            .collect(Collectors.joining(", "));

            String upsertSQL =
                    String.format(
                            " MERGE INTO %s TARGET"
                                    + " USING (%s) SOURCE"
                                    + " ON (%s) "
                                    + " WHEN MATCHED THEN"
                                    + " UPDATE SET %s"
                                    + " WHEN NOT MATCHED THEN"
                                    + " INSERT (%s) VALUES (%s)",
                            tableIdentifier(database, tableName),
                            usingClause,
                            onConditions,
                            updateSetClause,
                            insertFields,
                            insertValues);
            return Optional.of(upsertSQL);
        }
    }
}

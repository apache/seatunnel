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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Utilities to discovery matched tables. */
public class TableDiscoveryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    @SuppressWarnings("MagicNumber")
    public static List<TableId> listTables(JdbcConnection jdbc, RelationalTableFilters tableFilters)
            throws SQLException {
        LOG.info("Read list of Db2 CDC enabled tables");
        List<TableId> capturedTableIds =
                ((io.debezium.connector.db2.Db2Connection) jdbc)
                        .listOfChangeTables().stream()
                                .map(io.debezium.connector.db2.Db2ChangeTable::getSourceTableId)
                                .filter(tableFilters.dataCollectionFilter()::isIncluded)
                                .collect(Collectors.toList());
        capturedTableIds.forEach(
                tableId -> LOG.info("\t including '{}' for further processing", tableId));
        return capturedTableIds;
    }

    /**
     * Validates that every explicitly configured DB2 table is present in the capture-enabled table
     * set returned by Debezium discovery.
     *
     * <p>Db2 CDC discovery starts from ASN change-table metadata, so a requested table that is not
     * capture-enabled would otherwise disappear before split assignment. This guard keeps explicit
     * `table-names` semantics fail-fast instead of silently continuing with only the captured
     * subset.
     */
    public static void validateExplicitCaptureTables(
            List<String> configuredTables, List<TableId> capturedTableIds) {
        if (configuredTables == null || configuredTables.isEmpty()) {
            return;
        }

        Set<TableId> normalizedCapturedTables =
                capturedTableIds.stream()
                        .map(TableDiscoveryUtils::normalizeCapturedTableId)
                        .collect(Collectors.toSet());
        List<String> missingTables = new ArrayList<>();
        for (String configuredTable : configuredTables) {
            // Accept the DB2 folded spelling first and the configured spelling as a fallback, so
            // an object created with quoted lowercase identifiers that was matched unquoted before
            // folding was introduced still passes startup validation.
            TableId foldedTableId =
                    toConfiguredDb2TableId(normalizeConfiguredTableName(configuredTable));
            TableId configuredTableId = toConfiguredDb2TableId(configuredTable);
            if (!normalizedCapturedTables.contains(foldedTableId)
                    && !normalizedCapturedTables.contains(configuredTableId)) {
                missingTables.add(configuredTable);
            }
        }
        if (!missingTables.isEmpty()) {
            throw new SeaTunnelException(
                    "The following configured DB2 tables are not enabled for capture: "
                            + String.join(", ", missingTables));
        }
    }

    /**
     * Db2 Debezium metadata always uses an empty catalog because one connector instance captures a
     * single configured database. Explicit SeaTunnel table names still carry the database segment,
     * so startup validation needs to drop that catalog part before comparing with capture tables.
     */
    static TableId toConfiguredDb2TableId(String configuredTable) {
        int firstDot = configuredTable.indexOf('.');
        int secondDot = configuredTable.indexOf('.', firstDot + 1);
        if (firstDot < 0 || secondDot < 0 || secondDot == configuredTable.length() - 1) {
            throw new SeaTunnelException(
                    "DB2 CDC table-names must use database.schema.table format, but found: "
                            + configuredTable);
        }
        return new TableId(
                "",
                configuredTable.substring(firstDot + 1, secondDot),
                configuredTable.substring(secondDot + 1));
    }

    /**
     * Folds unquoted schema/table names to uppercase, the spelling DB2 gives unquoted identifiers.
     * The database is part of the SeaTunnel table identity and retains its configured spelling.
     * Double quotes are not interpreted by the catalog lookup or the capture filter, so a name that
     * contains a quote character is returned unchanged rather than partially folded.
     */
    public static String normalizeConfiguredTableName(String configuredTable) {
        if (configuredTable.contains("\"")) {
            return configuredTable;
        }
        int databaseEnd = configuredTable.indexOf('.') + 1;
        return configuredTable.substring(0, databaseEnd)
                + configuredTable.substring(databaseEnd).toUpperCase(Locale.ROOT);
    }

    /**
     * Resolves the spelling that catalog lookup, capture filtering and SeaTunnel table identity
     * share for one configured table.
     *
     * <p>Releases up to 3.0.0 passed the configured text through unchanged. The configured spelling
     * therefore wins whenever an object with exactly that spelling exists, so an upgraded or
     * restored job keeps selecting the same table even when an object with the folded spelling
     * exists as well. Only when the configured spelling does not exist is the DB2 folded
     * (uppercase) spelling used, which lets an unquoted lowercase name select an ordinary uppercase
     * object; if neither exists the folded name is returned and the existing lookups fail fast with
     * it. Names that folding leaves unchanged, including names containing quotes, never consult
     * {@code tableExists}.
     */
    public static String resolveConfiguredTableName(
            String configuredTable, Predicate<TablePath> tableExists) {
        String foldedTable = normalizeConfiguredTableName(configuredTable);
        if (foldedTable.equals(configuredTable)
                || tableExists.test(TablePath.of(configuredTable))) {
            return configuredTable;
        }
        return foldedTable;
    }

    /**
     * Debezium change-table discovery emits empty-catalog {@link TableId}s for Db2, so startup
     * validation normalizes both sides to the same shape before comparing them.
     */
    static TableId normalizeCapturedTableId(TableId tableId) {
        return new TableId("", tableId.schema(), tableId.table());
    }
}

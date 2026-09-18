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

import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
            if (!normalizedCapturedTables.contains(toConfiguredDb2TableId(configuredTable))) {
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
     * Debezium change-table discovery emits empty-catalog {@link TableId}s for Db2, so startup
     * validation normalizes both sides to the same shape before comparing them.
     */
    static TableId normalizeCapturedTableId(TableId tableId) {
        return new TableId("", tableId.schema(), tableId.table());
    }
}

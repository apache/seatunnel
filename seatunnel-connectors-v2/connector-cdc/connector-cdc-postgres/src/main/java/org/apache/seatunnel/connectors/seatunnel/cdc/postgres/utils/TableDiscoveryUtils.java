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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class TableDiscoveryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    /**
     * Reads the captured table ids from every database hosted by the connected PostgreSQL instance.
     *
     * <p>Filtering happens in two stages. First, {@code databaseFilter} is consulted per database
     * before any metadata query is issued: PostgreSQL cannot read another database's {@code
     * INFORMATION_SCHEMA} over the discovery connection, so probing foreign databases only produces
     * a warning per database (see <a
     * href="https://github.com/apache/seatunnel/issues/8184">#8184</a>). Second, tables read from
     * an allowed database are passed through {@code tableFilters.dataCollectionFilter()}, which
     * decides capture at table level.
     *
     * <p>The database predicate is deliberately kept outside the Debezium configuration: folding it
     * into {@code database.include.list} would make {@code dataCollectionFilter()} reject the
     * catalog-less {@link TableId}s used throughout the PostgreSQL connector.
     *
     * @param jdbc open connection to the database to discover
     * @param tableFilters table-level capture filter built from the connector config
     * @param databaseFilter predicate deciding which databases may be probed for tables
     * @return the deduplicated table ids eligible for capture, in discovery order
     */
    @SuppressWarnings("MagicNumber")
    public static List<TableId> listTables(
            JdbcConnection jdbc,
            RelationalTableFilters tableFilters,
            Predicate<String> databaseFilter)
            throws SQLException {
        // Use a LinkedHashSet to deduplicate table ids. Some PostgreSQL-compatible databases
        // (e.g. HighGo) return the same physical table several times from
        // INFORMATION_SCHEMA.TABLES, and duplicated TableId would break the downstream
        // Collectors.toMap() in PostgresIncrementalSource#tableChanges().
        // LinkedHashSet keeps the discovery order stable for standard PostgreSQL.
        final Set<TableId> capturedTableIds = new LinkedHashSet<>();
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();

        jdbc.query(
                "select datname from pg_database",
                rs -> {
                    while (rs.next()) {
                        databaseNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available databases is: {}", databaseNames);

        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // SqlServer, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in each database");
        for (String dbName : databaseNames) {
            if (!databaseFilter.test(dbName)) {
                LOG.debug("\t database '{}' is filtered out of capturing", dbName);
                continue;
            }
            try {
                jdbc.query(
                        "SELECT * FROM \""
                                + dbName
                                + "\".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';",
                        rs -> {
                            while (rs.next()) {
                                TableId tableId =
                                        new TableId(
                                                rs.getString(1), rs.getString(2), rs.getString(3));
                                if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                                    capturedTableIds.add(tableId);
                                    LOG.info("\t including '{}' for further processing", tableId);
                                } else {
                                    LOG.info("\t '{}' is filtered out of capturing", tableId);
                                }
                            }
                        });
            } catch (SQLException e) {
                // We were unable to execute the query or process the results, so skip this ...
                LOG.warn(
                        "\t skipping database '{}' due to error reading tables: {}",
                        dbName,
                        e.getMessage());
            }
        }
        return new ArrayList<>(capturedTableIds);
    }
}

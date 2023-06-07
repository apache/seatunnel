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
import java.util.List;

public class TableDiscoveryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    @SuppressWarnings("MagicNumber")
    public static List<TableId> listTables(
            JdbcConnection jdbc, RelationalTableFilters tableFilters, String database)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();

        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // Postgres, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in {}", database);
        try {
            jdbc.query(
                    "select\n"
                            + "n.nspname ,\n"
                            + "c.relname\n"
                            + "from\n"
                            + "pg_catalog.pg_class c\n"
                            + "left join pg_catalog.pg_namespace n on\n"
                            + "n.oid = c.relnamespace\n"
                            + "where\n"
                            + "c.relkind = 'r'\n"
                            + "and n.nspname <> 'pg_catalog'\n"
                            + "and n.nspname <> 'information_schema'\n"
                            + "and n.nspname !~ '^pg_toast';",
                    rs -> {
                        while (rs.next()) {
                            TableId tableId = new TableId(null, rs.getString(1), rs.getString(2));
                            if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                                capturedTableIds.add(tableId);
                                LOG.info("\t including '{}' forfurther processing", tableId);
                            } else {
                                LOG.info("\t '{}' is filtered out ofcapturing", tableId);
                            }
                        }
                    });
        } catch (SQLException e) {
            // We were unable to execute the query or process the results, so skip this ...
            LOG.warn(
                    "\t skipping database '{}' due to error reading tables: {}",
                    database,
                    e.getMessage());
        }
        return capturedTableIds;
    }
}

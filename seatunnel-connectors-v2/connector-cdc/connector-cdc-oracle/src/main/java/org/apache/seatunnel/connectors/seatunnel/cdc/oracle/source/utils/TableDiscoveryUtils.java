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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableDiscoveryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    @SuppressWarnings("MagicNumber")
    public static List<TableId> listTables(JdbcConnection jdbc, RelationalTableFilters tableFilters)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();

        Set<TableId> tableIdSet = new HashSet<>();
        String queryTablesSql =
                "SELECT OWNER ,TABLE_NAME,TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME NOT IN ('SYSTEM','SYSAUX')";
        try {
            jdbc.query(
                    queryTablesSql,
                    rs -> {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            String tableName = rs.getString(2);
                            TableId tableId = new TableId(jdbc.database(), schemaName, tableName);
                            tableIdSet.add(tableId);
                        }
                    });
        } catch (SQLException e) {
            LOG.warn(" SQL execute error, sql:{}", queryTablesSql, e);
        }

        for (TableId tableId : tableIdSet) {
            if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                capturedTableIds.add(tableId);
                LOG.info("\t including '{}' for further processing", tableId);
            } else {
                LOG.debug("\t '{}' is filtered out of capturing", tableId);
            }
        }

        return capturedTableIds;
    }
}

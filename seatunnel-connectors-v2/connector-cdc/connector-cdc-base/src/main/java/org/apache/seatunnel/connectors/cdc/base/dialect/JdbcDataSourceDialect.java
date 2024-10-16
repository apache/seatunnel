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

package org.apache.seatunnel.connectors.cdc.base.dialect;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface JdbcDataSourceDialect extends DataSourceDialect<JdbcSourceConfig> {

    /** Discovers the list of table to capture. */
    @Override
    List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig);

    /**
     * Creates and opens a new {@link JdbcConnection} backing connection pool.
     *
     * @param sourceConfig a basic source configuration.
     * @return a utility that simplifies using a JDBC connection.
     */
    JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig);

    /** Get a connection pool factory to create connection pool. */
    default JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        throw new UnsupportedOperationException();
    }

    /** Query and build the schema of table. */
    TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId);

    @Override
    FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase);

    @Override
    JdbcSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig);

    default Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId)
            throws SQLException {

        DatabaseMetaData metaData = jdbcConnection.connection().getMetaData();

        // seq -> column name
        List<Pair<Integer, String>> primaryKeyColumns = new ArrayList<>();
        String pkName = null;

        // According to the Javadoc of java.sql.DatabaseMetaData#getPrimaryKeys,
        // the returned primary key columns are ordered by COLUMN_NAME, not by KEY_SEQ.
        // We need to sort them based on the KEY_SEQ value.

        try (ResultSet rs =
                metaData.getPrimaryKeys(tableId.catalog(), tableId.schema(), tableId.table())) {
            while (rs.next()) {
                // all the PK_NAME should be the same
                pkName = rs.getString("PK_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                int keySeq = rs.getInt("KEY_SEQ");
                // KEY_SEQ is 1-based index
                primaryKeyColumns.add(Pair.of(keySeq, columnName));
            }
        }
        // initialize size
        List<String> pkFields =
                primaryKeyColumns.stream()
                        .sorted(Comparator.comparingInt(Pair::getKey))
                        .map(Pair::getValue)
                        .distinct()
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pkFields)) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(pkName, pkFields));
    }

    default List<ConstraintKey> getUniqueKeys(JdbcConnection jdbcConnection, TableId tableId)
            throws SQLException {
        return getConstraintKeys(jdbcConnection, tableId).stream()
                .filter(
                        constraintKey ->
                                constraintKey.getConstraintType()
                                        == ConstraintKey.ConstraintType.UNIQUE_KEY)
                .collect(Collectors.toList());
    }

    default Boolean isUniqueKey(JdbcConnection jdbcConnection, TableId tableId, String columnName)
            throws SQLException {
        boolean isUnique = false;
        if (StringUtils.isNotEmpty(columnName)) {
            DatabaseMetaData metaData = jdbcConnection.connection().getMetaData();
            ResultSet resultSet =
                    metaData.getIndexInfo(
                            tableId.catalog(), tableId.schema(), tableId.table(), false, false);

            while (resultSet.next()) {
                if (columnName.equalsIgnoreCase(resultSet.getString("COLUMN_NAME"))
                        && resultSet.getBoolean("NON_UNIQUE") == false) {
                    isUnique = true;
                    break;
                }
            }
        }
        return isUnique;
    }

    default List<ConstraintKey> getConstraintKeys(JdbcConnection jdbcConnection, TableId tableId)
            throws SQLException {
        DatabaseMetaData metaData = jdbcConnection.connection().getMetaData();

        try (ResultSet resultSet =
                metaData.getIndexInfo(
                        tableId.catalog(), tableId.schema(), tableId.table(), false, false)) {
            // index name -> index
            Map<String, ConstraintKey> constraintKeyMap = new HashMap<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                if (columnName == null) {
                    continue;
                }

                String indexName = resultSet.getString("INDEX_NAME");
                boolean noUnique = resultSet.getBoolean("NON_UNIQUE");

                ConstraintKey constraintKey =
                        constraintKeyMap.computeIfAbsent(
                                indexName,
                                s -> {
                                    ConstraintKey.ConstraintType constraintType =
                                            ConstraintKey.ConstraintType.INDEX_KEY;
                                    if (!noUnique) {
                                        constraintType = ConstraintKey.ConstraintType.UNIQUE_KEY;
                                    }
                                    return ConstraintKey.of(
                                            constraintType, indexName, new ArrayList<>());
                                });

                ConstraintKey.ColumnSortType sortType =
                        "A".equals(resultSet.getString("ASC_OR_DESC"))
                                ? ConstraintKey.ColumnSortType.ASC
                                : ConstraintKey.ColumnSortType.DESC;
                ConstraintKey.ConstraintKeyColumn constraintKeyColumn =
                        new ConstraintKey.ConstraintKeyColumn(columnName, sortType);
                constraintKey.getColumnNames().add(constraintKeyColumn);
            }
            return new ArrayList<>(constraintKeyMap.values());
        }
    }
}

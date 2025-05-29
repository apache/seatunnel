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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sourcetype.DatabaseTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);
    private static final String POINT = ".";
    private final JdbcSourceConfig jdbcSourceConfig;
    private final Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    @SneakyThrows
    public JdbcSource(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
        JdbcConnectionConfig jdbcConnectionConfig = jdbcSourceConfig.getJdbcConnectionConfig();
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcConnectionConfig.getUrl(), jdbcConnectionConfig.getCompatibleMode());
        JdbcConnectionProvider connectionProvider =
                jdbcDialect.getJdbcConnectionProvider(jdbcSourceConfig.getJdbcConnectionConfig());
        Connection connection = connectionProvider.getOrEstablishConnection();
        List<JdbcSourceTableConfig> jdbcSourceTableConfigs = new ArrayList<>();
        ResultSet rs = null;
        PreparedStatement ps = null;
        List<JdbcSourceTableConfig> tablePaths = jdbcSourceConfig.getTableConfigList();
        try {
            for (JdbcSourceTableConfig tableConfig : tablePaths) {
                List<String> schemaTables = new ArrayList<>();
                String tablePath = tableConfig.getTablePath();
                String query = tableConfig.getQuery();
                LOG.info("Processing table path: {}, custom query: {}", tablePath, query);
                String sql;
                if (StringUtils.isBlank(query)) {
                    String schemaName;
                    if (jdbcDialect.dialectName().startsWith(DatabaseTypeEnum.ORACLE.getValue())) {
                        schemaName = tablePath.split("\\.")[0];
                        sql = "SELECT OWNER, TABLE_NAME FROM dba_tables where OWNER=?";
                        ps = connection.prepareStatement(sql);
                        ps.setString(1, schemaName);
                        rs = ps.executeQuery();
                        while (rs.next()) {
                            // For Oracle: schema.table
                            String foundTable =
                                    rs.getString("OWNER") + POINT + rs.getString("TABLE_NAME");
                            schemaTables.add(foundTable);
                            LOG.info("Found table in Oracle: {}", foundTable);
                        }
                    } else if (jdbcDialect
                            .dialectName()
                            .equals(DatabaseTypeEnum.MYSQL.getValue())) {
                        schemaName = tablePath.split("\\.")[0];
                        sql =
                                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?";
                        ps = connection.prepareStatement(sql);
                        ps.setString(1, schemaName);
                        rs = ps.executeQuery();
                        while (rs.next()) {
                            // For MySQL: database.table
                            String foundTable =
                                    rs.getString("TABLE_SCHEMA")
                                            + POINT
                                            + rs.getString("TABLE_NAME");
                            schemaTables.add(foundTable);
                            LOG.info("Found table in MySQL: {}", foundTable);
                        }
                    } else if (jdbcDialect
                            .dialectName()
                            .equals(DatabaseTypeEnum.SQLSERVER.getValue())) {
                        String[] pathParts = tablePath.split("\\.");
                        String databaseName = pathParts[0];
                        schemaName = pathParts[1];
                        sql =
                                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
                                        + " WHERE TABLE_SCHEMA =? AND TABLE_CATALOG = DB_NAME()"
                                        + " AND TABLE_TYPE ='BASE TABLE'";
                        ps = connection.prepareStatement(sql);
                        ps.setString(1, schemaName);
                        rs = ps.executeQuery();
                        while (rs.next()) {
                            // For SQLServer: database.schema.table
                            String foundTable =
                                    databaseName
                                            + POINT
                                            + rs.getString("TABLE_SCHEMA")
                                            + POINT
                                            + rs.getString("TABLE_NAME");
                            schemaTables.add(foundTable);
                            LOG.info("Found table in SQLServer: {}", foundTable);
                        }
                    } else if (jdbcDialect
                            .dialectName()
                            .equals(DatabaseTypeEnum.POSTGRESQL.getValue())) {
                        String[] pathParts = tablePath.split("\\.");
                        String databaseName = pathParts[0];
                        schemaName = pathParts[1];
                        LOG.info(
                                "PostgreSQL: Processing database: {}, schema: {}",
                                databaseName,
                                schemaName);
                        sql =
                                "SELECT table_schema, table_name FROM information_schema.tables "
                                        + "WHERE table_schema = ? AND table_type = 'BASE TABLE' "
                                        + "AND table_catalog = current_database()";
                        ps = connection.prepareStatement(sql);
                        ps.setString(1, schemaName);
                        rs = ps.executeQuery();
                        while (rs.next()) {
                            // For PostgreSQL: database.schema.table
                            String foundTable =
                                    databaseName
                                            + POINT
                                            + rs.getString("table_schema")
                                            + POINT
                                            + rs.getString("table_name");
                            schemaTables.add(foundTable);
                            LOG.info("Found table in PostgreSQL: {}", foundTable);
                        }
                    } else {
                        throw new RuntimeException(
                                "not support dialect " + jdbcDialect.dialectName());
                    }
                    filterCapturedTablesByRegrex(
                            jdbcSourceTableConfigs, tableConfig, schemaTables, tablePath);
                } else {
                    jdbcSourceTableConfigs.add(tableConfig);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Regular expression match failed:", e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            connectionProvider.closeConnection();
        }

        this.jdbcSourceTables =
                JdbcCatalogUtils.getTables(
                        jdbcSourceConfig.getJdbcConnectionConfig(), jdbcSourceTableConfigs);
    }

    private void filterCapturedTablesByRegrex(
            List<JdbcSourceTableConfig> jdbcSourceTableConfigs,
            JdbcSourceTableConfig tableConfig,
            List<String> schemaTables,
            String tablePath) {
        LOG.info("Filtering tables with regex pattern: {}", tablePath);

        // Try exact match first
        if (schemaTables.contains(tablePath)) {
            LOG.info("Found exact table match: {}", tablePath);
            jdbcSourceTableConfigs.add(tableConfig);
            return;
        }

        // If no exact match, try pattern matching
        LOG.info(
                "No exact match found, trying pattern matching against {} tables",
                schemaTables.size());
        try {
            Pattern pattern = Pattern.compile(tablePath);
            for (String table : schemaTables) {
                if (pattern.matcher(table).find()) {
                    LOG.info("Found regex match table: {}", table);
                    JdbcSourceTableConfig jdbcSourceTableConfig = new JdbcSourceTableConfig();
                    jdbcSourceTableConfig.setTablePath(table);
                    if (tableConfig.getQuery() != null) {
                        jdbcSourceTableConfig.setQuery(tableConfig.getQuery());
                    }
                    jdbcSourceTableConfigs.add(jdbcSourceTableConfig);
                }
            }
            LOG.info("Total tables matched after filtering: {}", jdbcSourceTableConfigs.size());
        } catch (Exception e) {
            LOG.error("Error while matching regex pattern: {}", tablePath, e);
        }
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return jdbcSourceTables.values().stream()
                .map(JdbcSourceTable::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        Map<TablePath, CatalogTable> tables = new HashMap<>();
        for (TablePath tablePath : jdbcSourceTables.keySet()) {
            tables.put(tablePath, jdbcSourceTables.get(tablePath).getCatalogTable());
        }
        return new JdbcSourceReader(readerContext, jdbcSourceConfig, tables);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, jdbcSourceTables, null);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceState checkpointState)
            throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, jdbcSourceTables, checkpointState);
    }
}

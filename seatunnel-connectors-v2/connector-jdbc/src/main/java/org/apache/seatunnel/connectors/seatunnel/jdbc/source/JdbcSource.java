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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.GenericDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.SneakyThrows;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.table.CatalogOptions.TABLE_PATTERN;

public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private final JdbcSourceConfig jdbcSourceConfig;
    private final Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    @SneakyThrows
    public JdbcSource(TableSourceFactoryContext context) {
        this.jdbcSourceConfig = JdbcSourceConfig.of(context.getOptions());
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcSourceConfig.getJdbcConnectionConfig().getUrl(),
                        jdbcSourceConfig.getJdbcConnectionConfig().getDialect(),
                        jdbcSourceConfig.getJdbcConnectionConfig().getCompatibleMode());
        jdbcDialect.connectionUrlParse(
                jdbcSourceConfig.getJdbcConnectionConfig().getUrl(),
                jdbcSourceConfig.getJdbcConnectionConfig().getProperties(),
                jdbcDialect.defaultParameter());
        boolean usePattern = context.getOptions().getOptional(TABLE_PATTERN).isPresent();
        this.jdbcSourceTables =
                usePattern
                        ? createSourceTablesWithPattern(jdbcDialect, jdbcSourceConfig, context)
                        : createSourceTablesWithoutPattern();
    }

    private Map<TablePath, JdbcSourceTable> createSourceTablesWithPattern(
            JdbcDialect jdbcDialect,
            JdbcSourceConfig jdbcSourceConfig,
            TableSourceFactoryContext context) {
        if (jdbcDialect instanceof GenericDialect) {
            throw new IllegalStateException("GenericDialect does not support table patterns.");
        }
        ReadonlyConfig readonlyConfig =
                JdbcCatalogUtils.extractCatalogConfig(
                        jdbcSourceConfig.getJdbcConnectionConfig(), context.getOptions());
        List<CatalogTable> catalogTables =
                CatalogTableUtil.getCatalogTables(
                        jdbcDialect.dialectName(), readonlyConfig, context.getClassLoader());

        // Add a check to ensure the table config list is not empty
        if (jdbcSourceConfig.getTableConfigList().isEmpty()) {
            throw new IllegalStateException("Table config list cannot be empty.");
        }
        Map<String, JdbcSourceTableConfig> tableConfigMap =
                jdbcSourceConfig.getTableConfigList().stream()
                        .collect(
                                Collectors.toMap(
                                        JdbcSourceTableConfig::getTablePath,
                                        tableConfig -> tableConfig));
        return catalogTables.stream()
                .collect(
                        Collectors.toMap(
                                CatalogTable::getTablePath,
                                catalogTable -> {
                                    String fullName = catalogTable.getTablePath().getFullName();
                                    JdbcSourceTableConfig tableConfig =
                                            tableConfigMap.get(fullName);
                                    return JdbcSourceTable.builder()
                                            .tablePath(catalogTable.getTableId().toTablePath())
                                            .partitionColumn(tableConfig.getPartitionColumn())
                                            .partitionNumber(tableConfig.getPartitionNumber())
                                            .partitionStart(tableConfig.getPartitionStart())
                                            .partitionEnd(tableConfig.getPartitionEnd())
                                            .useSelectCount(tableConfig.getUseSelectCount())
                                            .skipAnalyze(tableConfig.getSkipAnalyze())
                                            .catalogTable(catalogTable)
                                            .build();
                                }));
    }

    private Map<TablePath, JdbcSourceTable> createSourceTablesWithoutPattern()
            throws SQLException, ClassNotFoundException {
        return JdbcCatalogUtils.getTables(
                jdbcSourceConfig.getJdbcConnectionConfig(), jdbcSourceConfig.getTableConfigList());
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

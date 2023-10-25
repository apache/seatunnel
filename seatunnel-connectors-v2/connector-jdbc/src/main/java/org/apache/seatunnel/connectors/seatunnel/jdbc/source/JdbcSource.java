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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSource.class)
@NoArgsConstructor
public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private JdbcSourceConfig jdbcSourceConfig;
    private Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    @SneakyThrows
    public JdbcSource(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
        this.jdbcSourceTables =
                JdbcCatalogUtils.getTables(
                        jdbcSourceConfig.getJdbcConnectionConfig(),
                        jdbcSourceConfig.getTableConfigList());
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @SneakyThrows
    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        ConfigValidator.of(config).validate(new JdbcSourceFactory().optionRule());
        this.jdbcSourceConfig = JdbcSourceConfig.of(config);
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcSourceConfig.getJdbcConnectionConfig().getUrl(),
                        jdbcSourceConfig.getJdbcConnectionConfig().getCompatibleMode());
        jdbcDialect.connectionUrlParse(
                jdbcSourceConfig.getJdbcConnectionConfig().getUrl(),
                jdbcSourceConfig.getJdbcConnectionConfig().getProperties(),
                jdbcDialect.defaultParameter());
        this.jdbcSourceTables =
                JdbcCatalogUtils.getTables(
                        jdbcSourceConfig.getJdbcConnectionConfig(),
                        jdbcSourceConfig.getTableConfigList());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return getProducedCatalogTables().get(0).getSeaTunnelRowType();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return jdbcSourceTables.values().stream()
                .map(e -> e.getCatalogTable())
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        Map<TablePath, SeaTunnelRowType> tables = new HashMap<>();
        for (TablePath tablePath : jdbcSourceTables.keySet()) {
            SeaTunnelRowType rowType =
                    jdbcSourceTables
                            .get(tablePath)
                            .getCatalogTable()
                            .getTableSchema()
                            .toPhysicalRowDataType();
            tables.put(tablePath, rowType);
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

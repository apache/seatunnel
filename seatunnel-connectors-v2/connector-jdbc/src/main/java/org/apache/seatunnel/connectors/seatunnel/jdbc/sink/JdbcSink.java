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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

@AutoService(SeaTunnelSink.class)
public class JdbcSink
        implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo>,
                SupportDataSaveMode {

    private SeaTunnelRowType seaTunnelRowType;

    private JobContext jobContext;

    private JdbcSinkConfig jdbcSinkConfig;

    private JdbcDialect dialect;

    private ReadonlyConfig config;

    private DataSaveMode dataSaveMode;

    private CatalogTable catalogTable;

    public JdbcSink(
            ReadonlyConfig config,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcDialect dialect,
            DataSaveMode dataSaveMode,
            CatalogTable catalogTable) {
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.dataSaveMode = dataSaveMode;
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    public JdbcSink() {}

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.dialect =
                JdbcDialectLoader.load(
                        jdbcSinkConfig.getJdbcConnectionConfig().getUrl(),
                        jdbcSinkConfig.getJdbcConnectionConfig().getCompatibleMode());
        this.dataSaveMode = DataSaveMode.KEEP_SCHEMA_AND_DATA;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(SinkWriter.Context context)
            throws IOException {
        SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> sinkWriter;
        if (jdbcSinkConfig.isExactlyOnce()) {
            sinkWriter =
                    new JdbcExactlyOnceSinkWriter(
                            context,
                            jobContext,
                            dialect,
                            jdbcSinkConfig,
                            seaTunnelRowType,
                            new ArrayList<>());
        } else {
            sinkWriter = new JdbcSinkWriter(context, dialect, jdbcSinkConfig, seaTunnelRowType);
        }

        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(
            SinkWriter.Context context, List<JdbcSinkState> states) throws IOException {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return new JdbcExactlyOnceSinkWriter(
                    context, jobContext, dialect, jdbcSinkConfig, seaTunnelRowType, states);
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>>
            createAggregatedCommitter() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkConfig));
        }
        return Optional.empty();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public DataSaveMode getUserConfigSaveMode() {
        return dataSaveMode;
    }

    @Override
    public void handleSaveMode(DataSaveMode saveMode) {
        if (catalogTable != null) {
            Map<String, String> catalogOptions = config.get(CatalogOptions.CATALOG_OPTIONS);
            if (catalogOptions != null) {
                String factoryId = catalogOptions.get(CommonOptions.FACTORY_ID.key());
                if (StringUtils.isBlank(jdbcSinkConfig.getDatabase())) {
                    return;
                }
                CatalogFactory catalogFactory =
                        discoverFactory(
                                Thread.currentThread().getContextClassLoader(),
                                CatalogFactory.class,
                                factoryId);
                if (catalogFactory != null) {
                    try (Catalog catalog =
                            catalogFactory.createCatalog(
                                    catalogFactory.factoryIdentifier(),
                                    ReadonlyConfig.fromMap(new HashMap<>(catalogOptions)))) {
                        catalog.open();
                        TablePath tablePath =
                                TablePath.of(
                                        jdbcSinkConfig.getDatabase()
                                                + "."
                                                + jdbcSinkConfig.getTable());
                        if (!catalog.databaseExists(jdbcSinkConfig.getDatabase())) {
                            catalog.createDatabase(tablePath, true);
                        }
                        if (!catalog.tableExists(tablePath)) {
                            catalog.createTable(tablePath, catalogTable, true);
                        }
                    } catch (Exception e) {
                        throw new JdbcConnectorException(HANDLE_SAVE_MODE_FAILED, e);
                    }
                }
            }
        }
    }
}

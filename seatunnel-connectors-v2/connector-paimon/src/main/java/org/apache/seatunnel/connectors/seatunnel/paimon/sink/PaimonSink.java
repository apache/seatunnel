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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.handler.PaimonSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;

import org.apache.paimon.table.Table;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public class PaimonSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        PaimonSinkState,
                        PaimonCommitInfo,
                        PaimonAggregatedCommitInfo>,
                SupportSaveMode,
                SupportLoadTable<Table>,
                SupportMultiTableSink {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "Paimon";

    private SeaTunnelRowType seaTunnelRowType;

    private Table table;

    private JobContext jobContext;

    private ReadonlyConfig readonlyConfig;

    private PaimonSinkConfig paimonSinkConfig;

    private CatalogTable catalogTable;

    private PaimonHadoopConfiguration paimonHadoopConfiguration;

    public PaimonSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.paimonSinkConfig = new PaimonSinkConfig(readonlyConfig);
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.paimonHadoopConfiguration = PaimonSecurityContext.loadHadoopConfig(paimonSinkConfig);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public PaimonSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new PaimonSinkWriter(
                context,
                table,
                seaTunnelRowType,
                jobContext,
                paimonSinkConfig,
                paimonHadoopConfiguration);
    }

    @Override
    public Optional<SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(
                new PaimonAggregatedCommitter(table, jobContext, paimonHadoopConfiguration));
    }

    @Override
    public SinkWriter<SeaTunnelRow, PaimonCommitInfo, PaimonSinkState> restoreWriter(
            SinkWriter.Context context, List<PaimonSinkState> states) throws IOException {
        return new PaimonSinkWriter(
                context,
                table,
                seaTunnelRowType,
                states,
                jobContext,
                paimonSinkConfig,
                paimonHadoopConfiguration);
    }

    @Override
    public Optional<Serializer<PaimonAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<PaimonCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        org.apache.seatunnel.api.table.factory.CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        org.apache.seatunnel.api.table.factory.CatalogFactory.class,
                        "Paimon");
        if (catalogFactory == null) {
            throw new PaimonConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(),
                            PluginType.SINK,
                            "Cannot find paimon catalog factory"));
        }
        org.apache.seatunnel.api.table.catalog.Catalog catalog =
                catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), readonlyConfig);
        return Optional.of(
                new PaimonSaveModeHandler(
                        this,
                        paimonSinkConfig.getSchemaSaveMode(),
                        paimonSinkConfig.getDataSaveMode(),
                        catalog,
                        catalogTable,
                        null));
    }

    @Override
    public void setLoadTable(Table table) {
        this.table = table;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}

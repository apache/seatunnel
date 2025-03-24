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

package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.config.DorisSinkOptions;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfoSerializer;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitter;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkState;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkStateSerializer;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public class DorisSink
        implements SeaTunnelSink<SeaTunnelRow, DorisSinkState, DorisCommitInfo, DorisCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink,
                SupportSchemaEvolutionSink {

    private final DorisSinkConfig dorisSinkConfig;
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private String jobId;

    public DorisSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.dorisSinkConfig = DorisSinkConfig.of(config);
    }

    @Override
    public String getPluginName() {
        return "Doris";
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public DorisSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new DorisSinkWriter(
                context, Collections.emptyList(), catalogTable, dorisSinkConfig, jobId);
    }

    @Override
    public SinkWriter<SeaTunnelRow, DorisCommitInfo, DorisSinkState> restoreWriter(
            SinkWriter.Context context, List<DorisSinkState> states) throws IOException {
        return new DorisSinkWriter(context, states, catalogTable, dorisSinkConfig, jobId);
    }

    @Override
    public Optional<Serializer<DorisSinkState>> getWriterStateSerializer() {
        return Optional.of(new DorisSinkStateSerializer());
    }

    @Override
    public Optional<SinkCommitter<DorisCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new DorisCommitter(dorisSinkConfig));
    }

    @Override
    public Optional<Serializer<DorisCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DorisCommitInfoSerializer());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        "Doris");
        if (catalogFactory == null) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, "Cannot find Doris catalog factory"));
        }

        Catalog catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), config);
        return Optional.of(
                new DefaultSaveModeHandler(
                        config.get(DorisSinkOptions.SCHEMA_SAVE_MODE),
                        config.get(DorisSinkOptions.DATA_SAVE_MODE),
                        catalog,
                        catalogTable,
                        config.get(DorisSinkOptions.CUSTOM_SQL)));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }
}

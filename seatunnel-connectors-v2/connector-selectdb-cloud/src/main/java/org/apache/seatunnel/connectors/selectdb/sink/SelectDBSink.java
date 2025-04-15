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

package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.*;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkConfig;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkOptions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfoSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitter;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkState;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkStateSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkWriter;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBStreamLoadSinkWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.CLUSTER_NAME;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.JDBC_URL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.LOAD_URL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkOptions.TABLE_IDENTIFIER;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkOptions.USERNAME;

public class SelectDBSink
        implements SeaTunnelSink<
                        SeaTunnelRow, SelectDBSinkState, SelectDBCommitInfo, SelectDBCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink,
                SupportSchemaEvolutionSink {

    @Deprecated
    private Config pluginConfig;
    @Deprecated
    private SeaTunnelRowType seaTunnelRowType;

    private final SelectDBSinkConfig selectDBSinkConfig;
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private String jobId;

    public SelectDBSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.selectDBSinkConfig = SelectDBSinkConfig.of(config);
    }

    @Override
    public String getPluginName() {
        return "SelectDBCloud";
    }

    @Override
    @Deprecated
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        JDBC_URL.key(),
                        LOAD_URL.key(),
                        CLUSTER_NAME.key(),
                        USERNAME.key(),
                        TABLE_IDENTIFIER.key());
        if (!result.isSuccess()) {
            throw new SelectDBConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    @Deprecated
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        // Compatible history
        if (pluginConfig.hasPath(TABLE_IDENTIFIER.key())) {
            SelectDBSinkWriter selectDBSinkWriter =
                    new SelectDBSinkWriter(
                            context, Collections.emptyList(), seaTunnelRowType, pluginConfig, jobId);
            selectDBSinkWriter.initializeLoad(Collections.emptyList());
            return selectDBSinkWriter;
        }

        return new SelectDBStreamLoadSinkWriter(
                context,
                Collections.emptyList(),
                catalogTable,
                selectDBSinkConfig,
                jobId);
    }

    @Override
    public SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState> restoreWriter(
            SinkWriter.Context context, List<SelectDBSinkState> states) throws IOException {
        if (pluginConfig.hasPath(TABLE_IDENTIFIER.key())) {
            SelectDBSinkWriter selectDBSinkWriter =
                    new SelectDBSinkWriter(context, states, seaTunnelRowType, pluginConfig, jobId);
            selectDBSinkWriter.initializeLoad(states);
            return selectDBSinkWriter;
        }

        return new SelectDBStreamLoadSinkWriter(
                context,
                states,
                catalogTable,
                selectDBSinkConfig,
                jobId);
    }

    @Override
    public Optional<Serializer<SelectDBSinkState>> getWriterStateSerializer() {
        return Optional.of(new SelectDBSinkStateSerializer());
    }

    @Override
    public Optional<SinkCommitter<SelectDBCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new SelectDBCommitter(selectDBSinkConfig));
    }

    @Override
    public Optional<Serializer<SelectDBCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new SelectDBCommitInfoSerializer());
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
                        "SelectDBCloud");
        if (catalogFactory == null) {
            throw new SelectDBConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, "Cannot find selectDB catalog factory"));
        }

        Catalog catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), config);
        return Optional.of(
                new DefaultSaveModeHandler(
                        config.get(SelectDBSinkOptions.SCHEMA_SAVE_MODE),
                        config.get(SelectDBSinkOptions.DATA_SAVE_MODE),
                        catalog,
                        catalogTable,
                        config.get(SelectDBSinkOptions.CUSTOM_SQL)));
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

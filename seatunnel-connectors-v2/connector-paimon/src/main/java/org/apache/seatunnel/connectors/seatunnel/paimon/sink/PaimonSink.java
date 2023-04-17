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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.HDFS_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.WAREHOUSE;

@AutoService(SeaTunnelSink.class)
public class PaimonSink
        implements SeaTunnelSink<
                SeaTunnelRow, PaimonSinkState, PaimonCommitInfo, PaimonAggregatedCommitInfo> {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "Paimon";

    private SeaTunnelRowType seaTunnelRowType;

    private Config pluginConfig;

    private Table table;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, WAREHOUSE.key(), DATABASE.key(), TABLE.key());
        if (!result.isSuccess()) {
            throw new PaimonConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        // initialize paimon table
        final String warehouse = pluginConfig.getString(WAREHOUSE.key());
        final String database = pluginConfig.getString(DATABASE.key());
        final String table = pluginConfig.getString(TABLE.key());
        final Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(WAREHOUSE.key(), warehouse);
        final Options options = Options.fromMap(optionsMap);
        final Configuration hadoopConf = new Configuration();
        if (pluginConfig.hasPath(HDFS_SITE_PATH.key())) {
            hadoopConf.addResource(new Path(pluginConfig.getString(HDFS_SITE_PATH.key())));
        }
        final CatalogContext catalogContext = CatalogContext.create(options, hadoopConf);
        try (Catalog catalog = CatalogFactory.createCatalog(catalogContext)) {
            Identifier identifier = Identifier.create(database, table);
            this.table = catalog.getTable(identifier);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Failed to get table [%s] from database [%s] on warehouse [%s]",
                            database, table, warehouse);
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.GET_TABLE_FAILED, errorMsg, e);
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, PaimonCommitInfo, PaimonSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new PaimonSinkWriter(context, table, seaTunnelRowType);
    }

    @Override
    public Optional<SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new PaimonAggregatedCommitter(table));
    }

    @Override
    public SinkWriter<SeaTunnelRow, PaimonCommitInfo, PaimonSinkState> restoreWriter(
            SinkWriter.Context context, List<PaimonSinkState> states) throws IOException {
        return new PaimonSinkWriter(context, table, seaTunnelRowType, states);
    }

    @Override
    public Optional<Serializer<PaimonAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<PaimonCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
}

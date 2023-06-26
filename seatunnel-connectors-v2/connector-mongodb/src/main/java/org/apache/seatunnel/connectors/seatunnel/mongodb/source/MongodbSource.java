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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbCollectionProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentRowDataDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.config.MongodbReadOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.enumerator.MongodbSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader.MongodbReader;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.SamplingSplitStrategy;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSource.class)
public class MongodbSource
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>>,
                SupportColumnProjection {

    private static final long serialVersionUID = 1L;

    private MongodbClientProvider clientProvider;

    private DocumentDeserializer<SeaTunnelRow> deserializer;

    private MongoSplitStrategy splitStrategy;

    private SeaTunnelRowType rowType;

    private MongodbReadOptions mongodbReadOptions;

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        MongodbConfig.ProcessConfig processConfig = new MongodbConfig.ProcessConfig(pluginConfig);
        clientProvider =
                MongodbCollectionProvider.builder()
                        .connectionString(pluginConfig.getString(MongodbConfig.URI.key()))
                        .database(pluginConfig.getString(MongodbConfig.DATABASE.key()))
                        .collection(pluginConfig.getString(MongodbConfig.COLLECTION.key()))
                        .build();

        if (pluginConfig.hasPath(CatalogTableUtil.SCHEMA.key())) {
            this.rowType = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        } else {
            this.rowType = CatalogTableUtil.buildSimpleTextSchema();
        }

        deserializer =
                processConfig.processConfigValueIfPresent(
                        MongodbConfig.FLAT_SYNC_STRING.key(),
                        value ->
                                new DocumentRowDataDeserializer(
                                        rowType.getFieldNames(), rowType, value),
                        MongodbConfig.FLAT_SYNC_STRING.defaultValue());

        SamplingSplitStrategy.Builder splitStrategyBuilder = SamplingSplitStrategy.builder();

        processConfig.processConfigValueIfPresent(
                MongodbConfig.MATCH_QUERY.key(), splitStrategyBuilder::setMatchQuery);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.SPLIT_KEY.key(), splitStrategyBuilder::setSplitKey);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.SPLIT_SIZE.key(), splitStrategyBuilder::setSizePerSplit);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.PROJECTION.key(), splitStrategyBuilder::setProjection);

        splitStrategy = splitStrategyBuilder.setClientProvider(clientProvider).build();

        MongodbReadOptions.MongoReadOptionsBuilder mongoReadOptionsBuilder =
                MongodbReadOptions.builder();

        processConfig.processConfigValueIfPresent(
                MongodbConfig.MAX_TIME_MIN.key(), mongoReadOptionsBuilder::setMaxTimeMin);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.FETCH_SIZE.key(), mongoReadOptionsBuilder::setFetchSize);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.CURSOR_NO_TIMEOUT.key(), mongoReadOptionsBuilder::setNoCursorTimeout);

        mongodbReadOptions = mongoReadOptionsBuilder.build();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return rowType;
    }

    @Override
    public SourceReader<SeaTunnelRow, MongoSplit> createReader(SourceReader.Context readerContext)
            throws Exception {
        return new MongodbReader(readerContext, clientProvider, deserializer, mongodbReadOptions);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> createEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext) throws Exception {
        return new MongodbSplitEnumerator(enumeratorContext, clientProvider, splitStrategy);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext,
            ArrayList<MongoSplit> checkpointState)
            throws Exception {
        return new MongodbSplitEnumerator(
                enumeratorContext, clientProvider, splitStrategy, checkpointState);
    }
}

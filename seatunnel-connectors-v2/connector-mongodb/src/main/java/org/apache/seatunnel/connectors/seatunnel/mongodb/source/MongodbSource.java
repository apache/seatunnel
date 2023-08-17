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

import org.bson.BsonDocument;

import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;

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
        if (pluginConfig.hasPath(MongodbConfig.URI.key())
                && pluginConfig.hasPath(MongodbConfig.DATABASE.key())
                && pluginConfig.hasPath(MongodbConfig.COLLECTION.key())) {
            String connection = pluginConfig.getString(MongodbConfig.URI.key());
            String database = pluginConfig.getString(MongodbConfig.DATABASE.key());
            String collection = pluginConfig.getString(MongodbConfig.COLLECTION.key());
            clientProvider =
                    MongodbCollectionProvider.builder()
                            .connectionString(connection)
                            .database(database)
                            .collection(collection)
                            .build();
        }
        if (pluginConfig.hasPath(CatalogTableUtil.SCHEMA.key())) {
            this.rowType = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        } else {
            this.rowType = CatalogTableUtil.buildSimpleTextSchema();
        }

        if (pluginConfig.hasPath(MongodbConfig.FLAT_SYNC_STRING.key())) {
            deserializer =
                    new DocumentRowDataDeserializer(
                            rowType.getFieldNames(),
                            rowType,
                            pluginConfig.getBoolean(MongodbConfig.FLAT_SYNC_STRING.key()));
        } else {
            deserializer =
                    new DocumentRowDataDeserializer(
                            rowType.getFieldNames(),
                            rowType,
                            MongodbConfig.FLAT_SYNC_STRING.defaultValue());
        }

        SamplingSplitStrategy.Builder splitStrategyBuilder = SamplingSplitStrategy.builder();
        if (pluginConfig.hasPath(MongodbConfig.MATCH_QUERY.key())) {
            splitStrategyBuilder.setMatchQuery(
                    BsonDocument.parse(pluginConfig.getString(MongodbConfig.MATCH_QUERY.key())));
        }

        List<String> fallbackKeys = MongodbConfig.MATCH_QUERY.getFallbackKeys();
        fallbackKeys.forEach(
                key -> {
                    if (pluginConfig.hasPath(key)) {
                        splitStrategyBuilder.setMatchQuery(
                                BsonDocument.parse(
                                        pluginConfig.getString(MongodbConfig.MATCH_QUERY.key())));
                    }
                });

        if (pluginConfig.hasPath(MongodbConfig.SPLIT_KEY.key())) {
            splitStrategyBuilder.setSplitKey(pluginConfig.getString(MongodbConfig.SPLIT_KEY.key()));
        }
        if (pluginConfig.hasPath(MongodbConfig.SPLIT_SIZE.key())) {
            splitStrategyBuilder.setSizePerSplit(
                    pluginConfig.getLong(MongodbConfig.SPLIT_SIZE.key()));
        }
        if (pluginConfig.hasPath(MongodbConfig.PROJECTION.key())) {
            splitStrategyBuilder.setProjection(
                    BsonDocument.parse(pluginConfig.getString(MongodbConfig.PROJECTION.key())));
        }
        splitStrategy = splitStrategyBuilder.setClientProvider(clientProvider).build();

        MongodbReadOptions.MongoReadOptionsBuilder mongoReadOptionsBuilder =
                MongodbReadOptions.builder();
        if (pluginConfig.hasPath(MongodbConfig.MAX_TIME_MIN.key())) {
            mongoReadOptionsBuilder.setMaxTimeMS(
                    pluginConfig.getLong(MongodbConfig.MAX_TIME_MIN.key()));
        }
        if (pluginConfig.hasPath(MongodbConfig.FETCH_SIZE.key())) {
            mongoReadOptionsBuilder.setFetchSize(
                    pluginConfig.getInt(MongodbConfig.FETCH_SIZE.key()));
        }
        if (pluginConfig.hasPath(MongodbConfig.CURSOR_NO_TIMEOUT.key())) {
            mongoReadOptionsBuilder.setNoCursorTimeout(
                    pluginConfig.getBoolean(MongodbConfig.CURSOR_NO_TIMEOUT.key()));
        }
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
    public SourceReader<SeaTunnelRow, MongoSplit> createReader(SourceReader.Context readerContext) {
        return new MongodbReader(readerContext, clientProvider, deserializer, mongodbReadOptions);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> createEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext) {
        return new MongodbSplitEnumerator(enumeratorContext, clientProvider, splitStrategy);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext,
            ArrayList<MongoSplit> checkpointState) {
        return new MongodbSplitEnumerator(
                enumeratorContext, clientProvider, splitStrategy, checkpointState);
    }
}

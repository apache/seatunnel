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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbCollectionProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentRowDataDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.config.MongodbReadOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.enumerator.MongodbSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader.MongodbReader;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.SamplingSplitStrategy;

import org.bson.BsonDocument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

public class MongodbSource
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>>,
                SupportColumnProjection {

    private static final long serialVersionUID = 1L;

    private final CatalogTable catalogTable;
    private final ReadonlyConfig options;

    public MongodbSource(CatalogTable catalogTable, ReadonlyConfig options) {
        this.catalogTable = catalogTable;
        this.options = options;
    }

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, MongoSplit> createReader(SourceReader.Context readerContext) {
        return new MongodbReader(
                readerContext,
                crateClientProvider(options),
                createDeserializer(options, catalogTable.getSeaTunnelRowType()),
                createMongodbReadOptions(options));
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> createEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext) {
        MongodbClientProvider clientProvider = crateClientProvider(options);
        return new MongodbSplitEnumerator(
                enumeratorContext, clientProvider, createSplitStrategy(options, clientProvider));
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext,
            ArrayList<MongoSplit> checkpointState) {
        MongodbClientProvider clientProvider = crateClientProvider(options);
        return new MongodbSplitEnumerator(
                enumeratorContext,
                clientProvider,
                createSplitStrategy(options, clientProvider),
                checkpointState);
    }

    private MongodbClientProvider crateClientProvider(ReadonlyConfig config) {
        return MongodbCollectionProvider.builder()
                .connectionString(config.get(MongodbConfig.URI))
                .database(config.get(MongodbConfig.DATABASE))
                .collection(config.get(MongodbConfig.COLLECTION))
                .build();
    }

    private DocumentRowDataDeserializer createDeserializer(
            ReadonlyConfig config, SeaTunnelRowType rowType) {
        return new DocumentRowDataDeserializer(
                rowType.getFieldNames(), rowType, config.get(MongodbConfig.FLAT_SYNC_STRING));
    }

    private MongoSplitStrategy createSplitStrategy(
            ReadonlyConfig config, MongodbClientProvider clientProvider) {
        SamplingSplitStrategy.Builder splitStrategyBuilder = SamplingSplitStrategy.builder();
        splitStrategyBuilder.setSplitKey(config.get(MongodbConfig.SPLIT_KEY));
        splitStrategyBuilder.setSizePerSplit(config.get(MongodbConfig.SPLIT_SIZE));
        config.getOptional(MongodbConfig.MATCH_QUERY)
                .ifPresent(s -> splitStrategyBuilder.setMatchQuery(BsonDocument.parse(s)));
        config.getOptional(MongodbConfig.PROJECTION)
                .ifPresent(s -> splitStrategyBuilder.setProjection(BsonDocument.parse(s)));
        return splitStrategyBuilder.setClientProvider(clientProvider).build();
    }

    private MongodbReadOptions createMongodbReadOptions(ReadonlyConfig config) {
        MongodbReadOptions.MongoReadOptionsBuilder mongoReadOptionsBuilder =
                MongodbReadOptions.builder();
        mongoReadOptionsBuilder.setMaxTimeMS(config.get(MongodbConfig.MAX_TIME_MIN));
        mongoReadOptionsBuilder.setFetchSize(config.get(MongodbConfig.FETCH_SIZE));
        mongoReadOptionsBuilder.setNoCursorTimeout(config.get(MongodbConfig.CURSOR_NO_TIMEOUT));
        return mongoReadOptionsBuilder.build();
    }
}

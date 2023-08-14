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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect;

import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.fetch.MongodbFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.fetch.MongodbScanFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.fetch.MongodbStreamFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamDescriptor;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.splitters.MongodbChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.CollectionDiscoveryUtils;

import org.bson.BsonDocument;

import com.mongodb.client.MongoClient;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DIALECT_NAME;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.CollectionDiscoveryUtils.collectionNames;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.CollectionDiscoveryUtils.collectionsFilter;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.CollectionDiscoveryUtils.databaseFilter;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.CollectionDiscoveryUtils.databaseNames;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.createMongoClient;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.getChangeStreamDescriptor;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.getCurrentClusterTime;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.getLatestResumeToken;

@Slf4j
public class MongodbDialect implements DataSourceDialect<MongodbSourceConfig> {

    private final Map<MongodbSourceConfig, CollectionDiscoveryUtils.CollectionDiscoveryInfo> cache =
            new ConcurrentHashMap<>();

    @Override
    public String getName() {
        return DIALECT_NAME;
    }

    @Override
    public List<TableId> discoverDataCollections(MongodbSourceConfig sourceConfig) {
        CollectionDiscoveryUtils.CollectionDiscoveryInfo discoveryInfo =
                discoverAndCacheDataCollections(sourceConfig);
        return discoveryInfo.getDiscoveredCollections().stream()
                .map(TableId::parse)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(MongodbSourceConfig sourceConfig) {
        // MongoDB's database names and collection names are case-sensitive.
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(MongodbSourceConfig sourceConfig) {
        return new MongodbChunkSplitter(sourceConfig);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(@Nonnull SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new MongodbScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new MongodbStreamFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(
            SourceSplitBase sourceSplitBase, MongodbSourceConfig sourceConfig) {
        CollectionDiscoveryUtils.CollectionDiscoveryInfo discoveryInfo =
                discoverAndCacheDataCollections(sourceConfig);
        ChangeStreamDescriptor changeStreamDescriptor =
                getChangeStreamDescriptor(
                        sourceConfig,
                        discoveryInfo.getDiscoveredDatabases(),
                        discoveryInfo.getDiscoveredCollections());
        return new MongodbFetchTaskContext(this, sourceConfig, changeStreamDescriptor);
    }

    private CollectionDiscoveryUtils.CollectionDiscoveryInfo discoverAndCacheDataCollections(
            MongodbSourceConfig sourceConfig) {
        return cache.computeIfAbsent(
                sourceConfig,
                config -> {
                    MongoClient mongoClient = createMongoClient(sourceConfig);
                    List<String> discoveredDatabases =
                            databaseNames(
                                    mongoClient, databaseFilter(sourceConfig.getDatabaseList()));
                    List<String> discoveredCollections =
                            collectionNames(
                                    mongoClient,
                                    discoveredDatabases,
                                    collectionsFilter(sourceConfig.getCollectionList()));
                    return new CollectionDiscoveryUtils.CollectionDiscoveryInfo(
                            discoveredDatabases, discoveredCollections);
                });
    }

    public ChangeStreamOffset displayCurrentOffset(MongodbSourceConfig sourceConfig) {
        MongoClient mongoClient = createMongoClient(sourceConfig);
        CollectionDiscoveryUtils.CollectionDiscoveryInfo discoveryInfo =
                discoverAndCacheDataCollections(sourceConfig);
        ChangeStreamDescriptor changeStreamDescriptor =
                getChangeStreamDescriptor(
                        sourceConfig,
                        discoveryInfo.getDiscoveredDatabases(),
                        discoveryInfo.getDiscoveredCollections());
        BsonDocument startupResumeToken = getLatestResumeToken(mongoClient, changeStreamDescriptor);

        ChangeStreamOffset changeStreamOffset;
        if (startupResumeToken != null) {
            changeStreamOffset = new ChangeStreamOffset(startupResumeToken);
            log.info(
                    "startup resume token={},change stream offset={}",
                    startupResumeToken,
                    changeStreamOffset);

        } else {
            changeStreamOffset = new ChangeStreamOffset(getCurrentClusterTime(mongoClient));
        }

        return changeStreamOffset;
    }
}

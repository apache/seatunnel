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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamDescriptor;

import org.apache.commons.lang3.StringUtils;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.ConnectionString;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ADD_NS_FIELD_NAME;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COMMAND_SUCCEED_FLAG;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DROPPED_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.MAX_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.MIN_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SHARD_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.UUID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.CollectionDiscoveryUtils.ADD_NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.CollectionDiscoveryUtils.includeListAsFlatPattern;

@Slf4j
public class MongodbUtils {

    private static final Map<TableId, MongoCollection<?>> cache = new ConcurrentHashMap<>();

    public static ChangeStreamDescriptor getChangeStreamDescriptor(
            @Nonnull MongodbSourceConfig sourceConfig,
            List<String> discoveredDatabases,
            List<String> discoveredCollections) {
        List<String> databaseList = sourceConfig.getDatabaseList();
        List<String> collectionList = sourceConfig.getCollectionList();

        ChangeStreamDescriptor changeStreamFilter;
        if (collectionList != null) {
            // Watching collections changes
            if (isIncludeListExplicitlySpecified(collectionList, discoveredCollections)) {
                changeStreamFilter =
                        ChangeStreamDescriptor.collection(
                                TableId.parse(discoveredCollections.get(0)));
            } else {
                Pattern namespaceRegex = includeListAsFlatPattern(collectionList);
                if (databaseList != null) {
                    if (isIncludeListExplicitlySpecified(databaseList, discoveredDatabases)) {
                        changeStreamFilter =
                                ChangeStreamDescriptor.database(
                                        discoveredDatabases.get(0), namespaceRegex);
                    } else {
                        Pattern databaseRegex = includeListAsFlatPattern(databaseList);
                        changeStreamFilter =
                                ChangeStreamDescriptor.deployment(databaseRegex, namespaceRegex);
                    }
                } else {
                    changeStreamFilter = ChangeStreamDescriptor.deployment(null, namespaceRegex);
                }
            }
        } else if (databaseList != null) {
            if (isIncludeListExplicitlySpecified(databaseList, discoveredDatabases)) {
                changeStreamFilter = ChangeStreamDescriptor.database(discoveredDatabases.get(0));
            } else {
                Pattern databaseRegex = includeListAsFlatPattern(databaseList);
                changeStreamFilter = ChangeStreamDescriptor.deployment(databaseRegex);
            }
        } else {
            // Watching all changes on the cluster
            changeStreamFilter = ChangeStreamDescriptor.deployment();
        }
        return changeStreamFilter;
    }

    public static boolean isIncludeListExplicitlySpecified(
            List<String> includeList, List<String> discoveredList) {
        if (includeList == null || includeList.size() != 1) {
            return false;
        }
        if (discoveredList == null || discoveredList.size() != 1) {
            return false;
        }
        String firstOfIncludeList = includeList.get(0);
        String firstOfDiscoveredList = discoveredList.get(0);
        return firstOfDiscoveredList.equals(firstOfIncludeList);
    }

    public static @Nonnull ChangeStreamIterable<Document> getChangeStreamIterable(
            MongodbSourceConfig sourceConfig, @Nonnull ChangeStreamDescriptor descriptor) {
        return getChangeStreamIterable(
                createMongoClient(sourceConfig),
                descriptor.getDatabase(),
                descriptor.getCollection(),
                descriptor.getDatabaseRegex(),
                descriptor.getNamespaceRegex(),
                sourceConfig.getBatchSize(),
                sourceConfig.isUpdateLookup());
    }

    public static @Nonnull ChangeStreamIterable<Document> getChangeStreamIterable(
            MongoClient mongoClient,
            @Nonnull ChangeStreamDescriptor descriptor,
            int batchSize,
            boolean updateLookup) {
        return getChangeStreamIterable(
                mongoClient,
                descriptor.getDatabase(),
                descriptor.getCollection(),
                descriptor.getDatabaseRegex(),
                descriptor.getNamespaceRegex(),
                batchSize,
                updateLookup);
    }

    public static @Nonnull ChangeStreamIterable<Document> getChangeStreamIterable(
            MongoClient mongoClient,
            String database,
            String collection,
            Pattern databaseRegex,
            Pattern namespaceRegex,
            int batchSize,
            boolean updateLookup) {
        ChangeStreamIterable<Document> changeStream;
        if (StringUtils.isNotEmpty(database) && StringUtils.isNotEmpty(collection)) {
            MongoCollection<Document> coll =
                    mongoClient.getDatabase(database).getCollection(collection);
            log.info("Preparing change stream for collection {}.{}", database, collection);
            changeStream = coll.watch();
        } else if (StringUtils.isNotEmpty(database) && namespaceRegex != null) {
            MongoDatabase db = mongoClient.getDatabase(database);
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(ADD_NS_FIELD);
            Bson nsFilter = regex(ADD_NS_FIELD_NAME, namespaceRegex);
            pipeline.add(match(nsFilter));
            log.info(
                    "Preparing change stream for database {} with namespace regex filter {}",
                    database,
                    namespaceRegex);
            changeStream = db.watch(pipeline);
        } else if (StringUtils.isNotEmpty(database)) {
            MongoDatabase db = mongoClient.getDatabase(database);
            log.info("Preparing change stream for database {}", database);
            changeStream = db.watch();
        } else if (namespaceRegex != null) {
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(ADD_NS_FIELD);

            Bson nsFilter = regex(ADD_NS_FIELD_NAME, namespaceRegex);
            if (databaseRegex != null) {
                Bson dbFilter = regex("ns.db", databaseRegex);
                nsFilter = and(dbFilter, nsFilter);
                log.info(
                        "Preparing change stream for deployment with"
                                + " database regex filter {} and namespace regex filter {}",
                        databaseRegex,
                        namespaceRegex);
            } else {
                log.info(
                        "Preparing change stream for deployment with namespace regex filter {}",
                        namespaceRegex);
            }

            pipeline.add(match(nsFilter));
            changeStream = mongoClient.watch(pipeline);
        } else if (databaseRegex != null) {
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(match(regex("ns.db", databaseRegex)));

            log.info(
                    "Preparing change stream for deployment  with database regex filter {}",
                    databaseRegex);
            changeStream = mongoClient.watch(pipeline);
        } else {
            log.info("Preparing change stream for deployment");
            changeStream = mongoClient.watch();
        }

        if (batchSize > 0) {
            changeStream.batchSize(batchSize);
        }

        if (updateLookup) {
            changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        return changeStream;
    }

    public static BsonDocument getLatestResumeToken(
            MongoClient mongoClient, ChangeStreamDescriptor descriptor) {
        ChangeStreamIterable<Document> changeStreamIterable =
                getChangeStreamIterable(mongoClient, descriptor, 1, false);

        // Nullable when no change record or postResumeToken (new in MongoDB 4.0.7).
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStreamCursor =
                changeStreamIterable.cursor()) {
            ChangeStreamDocument<Document> firstResult = changeStreamCursor.tryNext();

            return firstResult != null
                    ? firstResult.getResumeToken()
                    : changeStreamCursor.getResumeToken();
        }
    }

    public static boolean isCommandSucceed(BsonDocument commandResult) {
        return commandResult != null && COMMAND_SUCCEED_FLAG.equals(commandResult.getDouble("ok"));
    }

    public static String commandErrorMessage(BsonDocument commandResult) {
        return Optional.ofNullable(commandResult)
                .map(doc -> doc.getString("errmsg"))
                .map(BsonString::getValue)
                .orElse(null);
    }

    public static @Nonnull BsonDocument collStats(
            @Nonnull MongoClient mongoClient, @Nonnull TableId collectionId) {
        BsonDocument collStatsCommand =
                new BsonDocument("collStats", new BsonString(collectionId.table()));
        return mongoClient
                .getDatabase(collectionId.catalog())
                .runCommand(collStatsCommand, BsonDocument.class);
    }

    public static @Nonnull BsonDocument splitVector(
            MongoClient mongoClient,
            TableId collectionId,
            BsonDocument keyPattern,
            int maxChunkSizeMB) {
        return splitVector(mongoClient, collectionId, keyPattern, maxChunkSizeMB, null, null);
    }

    public static @Nonnull BsonDocument splitVector(
            @Nonnull MongoClient mongoClient,
            @Nonnull TableId collectionId,
            BsonDocument keyPattern,
            int maxChunkSizeMB,
            BsonDocument min,
            BsonDocument max) {
        BsonDocument splitVectorCommand =
                new BsonDocument("splitVector", new BsonString(collectionId.identifier()))
                        .append("keyPattern", keyPattern)
                        .append("maxChunkSize", new BsonInt32(maxChunkSizeMB));
        Optional.ofNullable(min).ifPresent(v -> splitVectorCommand.append(MIN_FIELD, v));
        Optional.ofNullable(max).ifPresent(v -> splitVectorCommand.append(MAX_FIELD, v));
        return mongoClient
                .getDatabase(collectionId.catalog())
                .runCommand(splitVectorCommand, BsonDocument.class);
    }

    public static BsonTimestamp getCurrentClusterTime(MongoClient mongoClient) {
        BsonDocument isMasterResult = isMaster(mongoClient);
        if (!isCommandSucceed(isMasterResult)) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "Failed to execute isMaster command: " + commandErrorMessage(isMasterResult));
        }
        return isMasterResult.getDocument("$clusterTime").getTimestamp("clusterTime");
    }

    public static @Nonnull BsonDocument isMaster(@Nonnull MongoClient mongoClient) {
        BsonDocument isMasterCommand = new BsonDocument("isMaster", new BsonInt32(1));
        return mongoClient.getDatabase("admin").runCommand(isMasterCommand, BsonDocument.class);
    }

    public static @Nonnull List<BsonDocument> readChunks(
            MongoClient mongoClient, @Nonnull BsonDocument collectionMetadata) {
        MongoCollection<BsonDocument> chunks =
                getMongoCollection(mongoClient, TableId.parse("config.chunks"), BsonDocument.class);
        List<BsonDocument> collectionChunks = new ArrayList<>();

        Bson filter =
                or(
                        new BsonDocument(NS_FIELD, collectionMetadata.get(ID_FIELD)),
                        // MongoDB 4.9.0 removed ns field of config.chunks collection, using
                        // collection's uuid instead.
                        // See: https://jira.mongodb.org/browse/SERVER-53105
                        new BsonDocument(UUID_FIELD, collectionMetadata.get(UUID_FIELD)));

        chunks.find(filter)
                .projection(include(MIN_FIELD, MAX_FIELD, SHARD_FIELD))
                .sort(ascending(MIN_FIELD))
                .into(collectionChunks);
        return collectionChunks;
    }

    public static BsonDocument readCollectionMetadata(
            MongoClient mongoClient, @Nonnull TableId collectionId) {
        MongoCollection<BsonDocument> collection =
                getMongoCollection(
                        mongoClient, TableId.parse("config.collections"), BsonDocument.class);

        return collection
                .find(eq(ID_FIELD, collectionId.identifier()))
                .projection(include(ID_FIELD, UUID_FIELD, DROPPED_FIELD, DOCUMENT_KEY))
                .first();
    }

    public static <T> @Nonnull MongoCollection<T> getMongoCollection(
            MongoClient mongoClient, TableId collectionId, Class<T> documentClass) {
        return getCollection(mongoClient, collectionId, documentClass);
    }

    @SuppressWarnings("unchecked")
    public static <T> @Nonnull MongoCollection<T> getCollection(
            MongoClient mongoClient, TableId collectionId, Class<T> documentClass) {
        MongoCollection<?> cachedCollection = cache.get(collectionId);
        if (cachedCollection == null) {
            MongoCollection<T> collection =
                    mongoClient
                            .getDatabase(collectionId.catalog())
                            .getCollection(collectionId.table(), documentClass);
            cache.put(collectionId, collection);
            return collection;
        }
        return (MongoCollection<T>) cachedCollection;
    }

    public static MongoClient createMongoClient(MongodbSourceConfig sourceConfig) {
        return MongodbClientProvider.INSTANCE.getOrCreateMongoClient(sourceConfig);
    }

    public static @Nonnull ConnectionString buildConnectionString(
            String username, String password, String hosts, String connectionOptions) {
        StringBuilder sb = new StringBuilder("mongodb://");

        if (hasCredentials(username, password)) {
            appendCredentials(sb, username, password);
        }

        sb.append(hosts);

        if (StringUtils.isNotEmpty(connectionOptions)) {
            sb.append("/?").append(connectionOptions);
        }

        return new ConnectionString(sb.toString());
    }

    private static boolean hasCredentials(String username, String password) {
        return StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password);
    }

    private static void appendCredentials(
            @Nonnull StringBuilder sb, String username, String password) {
        sb.append(encodeValue(username)).append(":").append(encodeValue(password)).append("@");
    }

    public static String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new MongodbConnectorException(ILLEGAL_ARGUMENT, e.getMessage());
        }
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbCollectionProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import org.bson.BsonDocument;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class MongodbSinkAggregatedCommitter
        implements SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo> {

    private static final long waitingTime = 5_000L;

    private static final long TRANSACTION_TIMEOUT_MS = 60_000L;

    private final boolean enableUpsert;

    private final String[] upsertKeys;

    private final MongodbClientProvider collectionProvider;

    private ClientSession clientSession;

    private MongoClient client;

    public MongodbSinkAggregatedCommitter(MongodbWriterOptions options) {
        this.enableUpsert = options.isUpsertEnable();
        this.upsertKeys = options.getPrimaryKey();
        this.collectionProvider =
                MongodbCollectionProvider.builder()
                        .connectionString(options.getConnectString())
                        .database(options.getDatabase())
                        .collection(options.getCollection())
                        .build();
    }

    @Override
    public List<MongodbAggregatedCommitInfo> commit(
            List<MongodbAggregatedCommitInfo> aggregatedCommitInfo) {
        return aggregatedCommitInfo.stream()
                .map(this::processAggregatedCommitInfo)
                .filter(
                        failedAggregatedCommitInfo ->
                                !failedAggregatedCommitInfo.getCommitInfos().isEmpty())
                .collect(Collectors.toList());
    }

    private MongodbAggregatedCommitInfo processAggregatedCommitInfo(
            MongodbAggregatedCommitInfo aggregatedCommitInfo) {
        List<MongodbCommitInfo> failedCommitInfos =
                aggregatedCommitInfo.getCommitInfos().stream()
                        .flatMap(
                                (Function<MongodbCommitInfo, Stream<List<DocumentBulk>>>)
                                        this::processCommitInfo)
                        .filter(failedDocumentBulks -> !failedDocumentBulks.isEmpty())
                        .map(MongodbCommitInfo::new)
                        .collect(Collectors.toList());

        return new MongodbAggregatedCommitInfo(failedCommitInfos);
    }

    private Stream<List<DocumentBulk>> processCommitInfo(MongodbCommitInfo commitInfo) {
        client = collectionProvider.getClient();
        clientSession = client.startSession();
        MongoCollection<BsonDocument> collection = collectionProvider.getDefaultCollection();
        return Stream.of(
                commitInfo.getDocumentBulks().stream()
                        .filter(bulk -> !bulk.getDocuments().isEmpty())
                        .filter(
                                bulk -> {
                                    try {
                                        CommittableTransaction transaction;
                                        if (enableUpsert) {
                                            transaction =
                                                    new CommittableUpsertTransaction(
                                                            collection,
                                                            bulk.getDocuments(),
                                                            upsertKeys);
                                        } else {
                                            transaction =
                                                    new CommittableTransaction(
                                                            collection, bulk.getDocuments());
                                        }

                                        int insertedDocs =
                                                clientSession.withTransaction(
                                                        transaction,
                                                        TransactionOptions.builder()
                                                                .readPreference(
                                                                        ReadPreference.primary())
                                                                .readConcern(ReadConcern.LOCAL)
                                                                .writeConcern(WriteConcern.MAJORITY)
                                                                .build());
                                        log.info(
                                                "Inserted {} documents into collection {}.",
                                                insertedDocs,
                                                collection.getNamespace());
                                        return false;
                                    } catch (Exception e) {
                                        log.error("Failed to commit with Mongo transaction.", e);
                                        return true;
                                    }
                                })
                        .collect(Collectors.toList()));
    }

    @Override
    public MongodbAggregatedCommitInfo combine(List<MongodbCommitInfo> commitInfos) {
        return new MongodbAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<MongodbAggregatedCommitInfo> aggregatedCommitInfo) {}

    @SneakyThrows
    @Override
    public void close() {
        long deadline = System.currentTimeMillis() + TRANSACTION_TIMEOUT_MS;
        while (clientSession.hasActiveTransaction() && System.currentTimeMillis() < deadline) {
            // wait for active transaction to finish or timeout
            Thread.sleep(waitingTime);
        }
        if (clientSession != null) {
            clientSession.close();
        }
        if (client != null) {
            client.close();
        }
    }
}

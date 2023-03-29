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
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import org.bson.Document;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MongodbSinkAggregatedCommitter
        implements SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo> {

    private final boolean enableUpsert;
    private final String[] upsertKeys;

    private final MongoClientProvider collectionProvider;

    private ClientSession clientSession;

    private MongoClient client;

    private static final long TRANSACTION_TIMEOUT_MS = 60_000L;

    public MongodbSinkAggregatedCommitter(MongodbWriterOptions options) {
        this.enableUpsert = options.isUpsertEnable();
        this.upsertKeys = options.getUpsertKey();
        this.collectionProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(options.getConnectString())
                        .database(options.getDatabase())
                        .collection(options.getCollection())
                        .build();
    }

    @Override
    public List<MongodbAggregatedCommitInfo> commit(
            List<MongodbAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
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
                        .map(this::processCommitInfo)
                        .filter(failedDocumentBulks -> !failedDocumentBulks.isEmpty())
                        .map(MongodbCommitInfo::new)
                        .collect(Collectors.toList());

        return new MongodbAggregatedCommitInfo(failedCommitInfos);
    }

    private List<DocumentBulk> processCommitInfo(MongodbCommitInfo commitInfo) {
        client = collectionProvider.getClient();
        clientSession = client.startSession();
        MongoCollection<Document> collection = collectionProvider.getDefaultCollection();
        return commitInfo.getDocumentBulks().stream()
                .filter(bulk -> !bulk.getDocuments().isEmpty())
                .filter(
                        bulk -> {
                            try {
                                CommittableTransaction transaction;
                                if (enableUpsert) {
                                    transaction =
                                            new CommittableUpsertTransaction(
                                                    collection, bulk.getDocuments(), upsertKeys);
                                } else {
                                    transaction =
                                            new CommittableTransaction(
                                                    collection, bulk.getDocuments());
                                }

                                int insertedDocs =
                                        clientSession.withTransaction(
                                                transaction,
                                                TransactionOptions.builder()
                                                        .readPreference(ReadPreference.primary())
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
                .collect(Collectors.toList());
    }

    @Override
    public MongodbAggregatedCommitInfo combine(List<MongodbCommitInfo> commitInfos) {
        return new MongodbAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<MongodbAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        aggregatedCommitInfo.stream()
                .flatMap(aggregatedCommit -> aggregatedCommit.getCommitInfos().stream())
                .forEach(
                        commitInfo -> {
                            try (ClientSession clientSession =
                                    collectionProvider.getClient().startSession()) {
                                clientSession.abortTransaction();
                            }
                        });
    }

    @SneakyThrows
    @Override
    public void close() throws IOException {
        long deadline = System.currentTimeMillis() + TRANSACTION_TIMEOUT_MS;
        while (clientSession.hasActiveTransaction() && System.currentTimeMillis() < deadline) {
            // wait for active transaction to finish or timeout
            Thread.sleep(5_000L);
        }
        clientSession.close();
        client.close();
    }
}

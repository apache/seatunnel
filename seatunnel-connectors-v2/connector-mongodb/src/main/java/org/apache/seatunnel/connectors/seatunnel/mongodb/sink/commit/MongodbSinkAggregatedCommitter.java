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
import com.mongodb.client.model.WriteModel;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MongodbSinkAggregatedCommitter
        implements SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo> {

    private final MongoClientProvider collectionProvider;

    private ClientSession clientSession;

    private MongoClient client;

    private static final long TRANSACTION_TIMEOUT_MS = 60_000L;

    public MongodbSinkAggregatedCommitter(MongodbWriterOptions options) {
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

    private List<WriteModel<BsonDocument>> processCommitInfo(MongodbCommitInfo commitInfo) {
        client = collectionProvider.getClient();
        clientSession = client.startSession();
        MongoCollection<BsonDocument> collection = collectionProvider.getDefaultCollection();
        return commitInfo.getDocumentBulks().stream()
                .filter(
                        bulk -> {
                            try {
                                CommittableTransaction transaction;
                                transaction =
                                        new CommittableTransaction(
                                                collection, Collections.singletonList(bulk));
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
        if (clientSession != null) {
            long deadline = System.currentTimeMillis() + TRANSACTION_TIMEOUT_MS;
            while (clientSession.hasActiveTransaction() && System.currentTimeMillis() < deadline) {
                log.debug("Waiting for active transaction to finish");
                try {
                    Thread.sleep(5_000L);
                } catch (InterruptedException e) {
                    log.error("Interrupted while waiting for active transaction to finish", e);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            clientSession.close();
        }
        if (client != null) {
            client.close();
        }
    }
}

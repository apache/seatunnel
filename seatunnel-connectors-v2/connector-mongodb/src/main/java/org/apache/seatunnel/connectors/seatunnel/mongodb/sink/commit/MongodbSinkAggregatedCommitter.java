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
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongoKeyExtractor;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import org.bson.BsonDocument;

import com.mongodb.MongoException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.exception.CommonErrorCode.FLUSH_DATA_FAILED;

@Slf4j
public class MongodbSinkAggregatedCommitter
        implements SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo> {

    private static final long TRANSACTION_TIMEOUT_MS = 60_000L;

    private final MongoClientProvider collectionProvider;

    private ClientSession clientSession;

    private MongoClient client;

    private final MongodbWriterOptions options;

    public MongodbSinkAggregatedCommitter(MongodbWriterOptions options) {
        this.collectionProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(options.getConnectString())
                        .database(options.getDatabase())
                        .collection(options.getCollection())
                        .build();
        this.options = options;
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
        List<MongodbCommitInfo> failedCommitInfos = new ArrayList<>();
        for (MongodbCommitInfo commitInfo : aggregatedCommitInfo.getCommitInfos()) {
            try {
                processCommitInfo(commitInfo);
            } catch (MongoException e) {
                log.error("Error occurred while processing commit info", e);
                failedCommitInfos.add(commitInfo);
            }
        }
        return new MongodbAggregatedCommitInfo(failedCommitInfos);
    }

    public void processCommitInfo(MongodbCommitInfo commitInfo) {
        client = collectionProvider.getClient();
        clientSession = client.startSession();
        MongoCollection<BsonDocument> collection = collectionProvider.getDefaultCollection();
        try {
            CommittableTransaction transaction;
            if (options.isUpsertEnable()) {
                transaction =
                        new CommittableUpsertTransaction(
                                collection,
                                commitInfo.getDocumentBulks(),
                                new MongoKeyExtractor(options));
            } else {
                transaction = new CommittableTransaction(collection, commitInfo.getDocumentBulks());
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
        } catch (MongoException e) {
            throw new MongodbConnectorException(
                    FLUSH_DATA_FAILED, "Error occurred while processing commit info", e);
        } finally {
            clientSession.close();
        }
    }

    @Override
    public MongodbAggregatedCommitInfo combine(List<MongodbCommitInfo> commitInfos) {
        return new MongodbAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<MongodbAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {}

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

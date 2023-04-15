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
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MongodbSinkAggregatedCommitter
        implements SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo> {

    private final boolean enableUpsert;
    private final String[] upsertKeys;

    MongoClientProvider collectionProvider;

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
        ClientSession clientSession = collectionProvider.getClient().startSession();
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

    @Override
    public void close() throws IOException {}
}

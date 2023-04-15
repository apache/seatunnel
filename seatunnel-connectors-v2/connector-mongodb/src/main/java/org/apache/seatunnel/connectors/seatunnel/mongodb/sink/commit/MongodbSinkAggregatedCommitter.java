package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.CommittableTransaction;
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
                        .map(
                                failedDocumentBulks ->
                                        new MongodbCommitInfo(
                                                aggregatedCommitInfo
                                                        .getCommitInfos()
                                                        .get(0)
                                                        .getClientProvider(),
                                                failedDocumentBulks))
                        .collect(Collectors.toList());

        return new MongodbAggregatedCommitInfo(failedCommitInfos);
    }

    private List<DocumentBulk> processCommitInfo(MongodbCommitInfo commitInfo) {
        ClientSession clientSession = commitInfo.getClientProvider().getClient().startSession();
        MongoCollection<Document> collection =
                commitInfo.getClientProvider().getDefaultCollection();

        return commitInfo.getDocumentBulks().stream()
                .filter(bulk -> !bulk.getDocuments().isEmpty())
                .filter(
                        bulk -> {
                            try {
                                CommittableTransaction transaction =
                                        new CommittableTransaction(collection, bulk.getDocuments());
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
                                    commitInfo.getClientProvider().getClient().startSession()) {
                                clientSession.abortTransaction();
                            }
                        });
    }

    @Override
    public void close() throws IOException {}
}

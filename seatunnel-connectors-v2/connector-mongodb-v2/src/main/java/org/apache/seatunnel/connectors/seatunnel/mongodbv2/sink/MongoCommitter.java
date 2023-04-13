package org.apache.seatunnel.connectors.seatunnel.mongodbv2.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.internal.MongoClientProvider;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * MongoCommitter flushes data to MongoDB in a transaction. Due to MVCC implementation of MongoDB, a
 * transaction is not recommended to be large.
 */
public class MongoCommitter implements SinkCommitter<MongodbCommitInfo> {

    private final MongoClient client;

    private final MongoCollection<Document> collection;

    private final ClientSession session;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoCommitter.class);

    public MongoCommitter(
            MongoClientProvider clientProvider, boolean enableUpsert, String[] upsertKeys) {
        this.client = clientProvider.getClient();
        this.collection = clientProvider.getDefaultCollection();
        this.session = client.startSession();
        this.enableUpsert = enableUpsert;
        this.upsertKeys = upsertKeys;
    }

    private TransactionOptions txnOptions =
            TransactionOptions.builder()
                    .readPreference(ReadPreference.primary())
                    .readConcern(ReadConcern.LOCAL)
                    .writeConcern(WriteConcern.MAJORITY)
                    .build();

    private final boolean enableUpsert;
    private final String[] upsertKeys;

    private static final long TRANSACTION_TIMEOUT_MS = 60_000L;

    @Override
    public List<MongodbCommitInfo> commit(List<MongodbCommitInfo> commitInfos) throws IOException {
        MongodbCommitInfo mongodbCommitInfo = new MongodbCommitInfo();
        for (MongodbCommitInfo commitInfo : commitInfos) {
            for (DocumentBulk bulk : commitInfo.pendingBulks) {
                if (bulk.getDocuments().size() > 0) {
                    CommittableTransaction transaction;
                    if (enableUpsert) {
                        transaction =
                                new CommittableUpsertTransaction(
                                        collection, bulk.getDocuments(), upsertKeys);
                    } else {
                        transaction = new CommittableTransaction(collection, bulk.getDocuments());
                    }
                    try {
                        int insertedDocs = session.withTransaction(transaction, txnOptions);
                        LOGGER.info(
                                "Inserted {} documents into collection {}.",
                                insertedDocs,
                                collection.getNamespace());
                    } catch (Exception e) {
                        // save to a new list that would be retried
                        LOGGER.error("Failed to commit with Mongo transaction.", e);
                        MongodbCommitInfo.of(Collections.singletonList(bulk));
                    }
                }
            }
        }
        return Collections.singletonList(mongodbCommitInfo);
    }

    @Override
    public void abort(List<MongodbCommitInfo> commitInfos) throws IOException {}
}

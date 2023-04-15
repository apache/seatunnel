package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;

import java.util.ArrayList;
import java.util.List;

public class CommittableUpsertTransaction extends CommittableTransaction {

    private final String[] upsertKeys;
    private final UpdateOptions updateOptions = new UpdateOptions();
    private final BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();

    public CommittableUpsertTransaction(
            MongoCollection<Document> collection, List<Document> documents, String[] upsertKeys) {
        super(collection, documents);
        this.upsertKeys = upsertKeys;
        updateOptions.upsert(true);
        bulkWriteOptions.ordered(true);
    }

    @Override
    public Integer execute() {
        List<UpdateOneModel<Document>> upserts = new ArrayList<>();
        for (Document document : bufferedDocuments) {
            List<Bson> filters = new ArrayList<>(upsertKeys.length);
            for (String upsertKey : upsertKeys) {
                Object o = document.get(upsertKey);
                Bson eq = Filters.eq(upsertKey, o);
                filters.add(eq);
            }
            Document update = new Document();
            update.append("$set", document);
            Bson filter = Filters.and(filters);
            UpdateOneModel<Document> updateOneModel =
                    new UpdateOneModel<>(filter, update, updateOptions);
            upserts.add(updateOneModel);
        }

        BulkWriteResult bulkWriteResult = collection.bulkWrite(upserts, bulkWriteOptions);
        return bulkWriteResult.getUpserts().size() + bulkWriteResult.getInsertedCount();
    }
}

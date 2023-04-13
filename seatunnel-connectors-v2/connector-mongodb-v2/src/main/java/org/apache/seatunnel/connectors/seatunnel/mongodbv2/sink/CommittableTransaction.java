package org.apache.seatunnel.connectors.seatunnel.mongodbv2.sink;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.result.InsertManyResult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** An simple implementation of Mongo transaction body. */
public class CommittableTransaction implements TransactionBody<Integer>, Serializable {

    protected final MongoCollection<Document> collection;

    protected List<Document> bufferedDocuments = new ArrayList<>(BUFFER_INIT_SIZE);

    private static final int BUFFER_INIT_SIZE = 1024;

    public CommittableTransaction(MongoCollection<Document> collection, List<Document> documents) {
        this.collection = collection;
        this.bufferedDocuments.addAll(documents);
    }

    @Override
    public Integer execute() {
        InsertManyResult result = collection.insertMany(bufferedDocuments);
        return result.getInsertedIds().size();
    }
}

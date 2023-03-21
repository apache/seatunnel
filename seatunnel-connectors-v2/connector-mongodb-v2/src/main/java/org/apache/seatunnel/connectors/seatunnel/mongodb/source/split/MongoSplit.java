package org.apache.seatunnel.connectors.seatunnel.mongodb.source.split;

import org.apache.seatunnel.api.source.SourceSplit;
import org.bson.BsonDocument;

/**
 * MongoSplit is composed a query and a start offset.
 **/
public class MongoSplit implements SourceSplit {

    private final String splitId;

    private final BsonDocument query;

    private final BsonDocument projection;

    private long startOffset;

    public MongoSplit(String splitId, BsonDocument query, BsonDocument projection) {
        this(splitId, query, projection, 0);
    }

    public MongoSplit(String splitId, BsonDocument query, BsonDocument projection, long startOffset) {
        this.splitId = splitId;
        this.query = query;
        this.projection= projection;
        this.startOffset = startOffset;
    }

    public BsonDocument getQuery() {return query;}

    public BsonDocument getProjection() {return projection;}

    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}

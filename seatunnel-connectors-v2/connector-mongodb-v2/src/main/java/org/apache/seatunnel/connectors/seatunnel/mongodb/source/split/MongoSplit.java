package org.apache.seatunnel.connectors.seatunnel.mongodb.source.split;

import org.apache.seatunnel.api.source.SourceSplit;

import org.bson.BsonDocument;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** MongoSplit is composed a query and a start offset. */
@Getter
@AllArgsConstructor
public class MongoSplit implements SourceSplit {

    private final String splitId;

    private final BsonDocument query;

    private final BsonDocument projection;

    private final long startOffset;

    @Override
    public String splitId() {
        return splitId;
    }
}

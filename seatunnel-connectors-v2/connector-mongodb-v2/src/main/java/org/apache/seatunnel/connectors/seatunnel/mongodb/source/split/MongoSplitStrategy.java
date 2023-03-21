package org.apache.seatunnel.connectors.seatunnel.mongodb.source.split;

import java.util.List;

/**
 * MongoSplitStrategy defines how to partition a Mongo data set into {@link MongoSplit}s.
 **/
public interface MongoSplitStrategy {

    List<MongoSplit> split();

}

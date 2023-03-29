package org.apache.seatunnel.connectors.seatunnel.mongodb.source.split;

import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;

import org.apache.commons.lang3.tuple.ImmutablePair;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * A split strategy that splits a collection via key ranges (default to '_id' field). The ranges are
 * automatically calculated based on collection stats and sampling.
 */
public class SamplingSplitStrategy implements MongoSplitStrategy, Serializable {

    private final MongoClientProvider clientProvider;

    private final String splitKey;

    private final BsonDocument matchQuery;

    private final BsonDocument projection;

    private final long samplesPerSplit;

    private final long sizePerSplit;

    SamplingSplitStrategy(
            MongoClientProvider clientProvider,
            String splitKey,
            BsonDocument matchQuery,
            BsonDocument projection,
            long samplesPerSplit,
            long sizePerSplit) {
        this.clientProvider = clientProvider;
        this.splitKey = splitKey;
        this.matchQuery = matchQuery;
        this.projection = projection;
        this.samplesPerSplit = samplesPerSplit;
        this.sizePerSplit = sizePerSplit;
    }

    @Override
    public List<MongoSplit> split() {
        ImmutablePair<Long, Long> numAndAvgSize = getDocumentNumAndAvgSize();
        long count = numAndAvgSize.left;
        long avgSize = numAndAvgSize.right;

        long numDocumentsPerSplit = sizePerSplit / avgSize;
        int numSplits = (int) Math.ceil((double) count / numDocumentsPerSplit);
        int numSamples = (int) Math.floor(samplesPerSplit * numSplits);

        if (numSplits == 0) {
            return Lists.newArrayList();
        }
        if (numSplits == 1) {
            return Lists.newArrayList(
                    MongoSplitUtils.createMongoSplit(
                            0, matchQuery, projection, splitKey, null, null));
        }
        List<Document> samples = sampleCollection(numSamples);
        if (samples.size() == 0) {
            return Collections.emptyList();
        }
        List<Object> rightBoundaries =
                IntStream.range(0, samples.size())
                        .filter(
                                i ->
                                        i % samplesPerSplit == 0
                                                || !matchQuery.isEmpty() && i == count - 1)
                        .mapToObj(i -> samples.get(i).get(splitKey))
                        .collect(toList());

        return createSplits(splitKey, rightBoundaries);
    }

    private ImmutablePair<Long, Long> getDocumentNumAndAvgSize() {
        String collectionName =
                clientProvider.getDefaultCollection().getNamespace().getCollectionName();
        BsonDocument statsCmd = new BsonDocument("collStats", new BsonString(collectionName));
        Document res = clientProvider.getDefaultDatabase().runCommand(statsCmd);
        long total = res.getInteger("count");
        long avgDocumentBytes = res.getInteger("avgObjSize");
        if (matchQuery == null || matchQuery.isEmpty()) {
            return ImmutablePair.of(total, avgDocumentBytes);
        } else {
            return ImmutablePair.of(
                    clientProvider.getDefaultCollection().countDocuments(matchQuery),
                    avgDocumentBytes);
        }
    }

    private List<Document> sampleCollection(int numSamples) {
        return clientProvider
                .getDefaultCollection()
                .aggregate(
                        Lists.newArrayList(
                                Aggregates.match(matchQuery),
                                Aggregates.sample(numSamples),
                                Aggregates.project(Projections.include(splitKey)),
                                Aggregates.sort(Sorts.ascending(splitKey))))
                .allowDiskUse(true)
                .into(Lists.newArrayList());
    }

    private List<MongoSplit> createSplits(String splitKey, List<Object> rightBoundaries) {
        if (rightBoundaries.size() == 0) {
            return Collections.emptyList();
        }
        List<MongoSplit> splits = Lists.newArrayList();
        for (int i = 0; i < rightBoundaries.size(); i++) {
            Object min = i > 0 ? rightBoundaries.get(i - 1) : null;
            splits.add(
                    MongoSplitUtils.createMongoSplit(
                            i, matchQuery, projection, splitKey, min, rightBoundaries.get(i)));
        }
        Object lastBoundary = rightBoundaries.get(rightBoundaries.size() - 1);
        splits.add(
                MongoSplitUtils.createMongoSplit(
                        splits.size(), matchQuery, projection, splitKey, lastBoundary, null));
        return splits;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private MongoClientProvider clientProvider;

        private String splitKey;

        private BsonDocument matchQuery;

        private BsonDocument projection;

        private long samplesPerSplit;

        private long sizePerSplit;

        private static final BsonDocument EMPTY_MATCH_QUERY = new BsonDocument();

        private static final BsonDocument EMPTY_PROJECTION = new BsonDocument();

        private static final String DEFAULT_SPLIT_KEY = "_id";

        private static final long DEFAULT_SAMPLES_PER_SPLIT = 10;

        private static final long DEFAULT_SIZE_PER_SPLIT = 64 * 1024 * 1024;

        Builder() {
            this.clientProvider = null;
            this.splitKey = DEFAULT_SPLIT_KEY;
            this.matchQuery = EMPTY_MATCH_QUERY;
            this.projection = EMPTY_PROJECTION;
            this.samplesPerSplit = DEFAULT_SAMPLES_PER_SPLIT;
            this.sizePerSplit = DEFAULT_SIZE_PER_SPLIT;
        }

        public Builder setClientProvider(MongoClientProvider clientProvider) {
            this.clientProvider = clientProvider;
            return this;
        }

        public Builder setSplitKey(String splitKey) {
            this.splitKey = splitKey;
            return this;
        }

        public Builder setMatchQuery(BsonDocument matchQuery) {
            this.matchQuery = matchQuery;
            return this;
        }

        public Builder setProjection(BsonDocument projection) {
            this.projection = projection;
            return this;
        }

        public Builder setSamplesPerSplit(long samplesPerSplit) {
            this.samplesPerSplit = samplesPerSplit;
            return this;
        }

        public Builder setSizePerSplit(long sizePerSplit) {
            this.sizePerSplit = sizePerSplit;
            return this;
        }

        public SamplingSplitStrategy build() {
            Preconditions.checkNotNull(clientProvider);
            return new SamplingSplitStrategy(
                    clientProvider,
                    splitKey,
                    matchQuery,
                    projection,
                    samplesPerSplit,
                    sizePerSplit);
        }
    }
}

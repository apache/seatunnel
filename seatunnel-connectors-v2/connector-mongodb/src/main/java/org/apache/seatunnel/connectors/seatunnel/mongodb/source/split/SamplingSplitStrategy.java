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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source.split;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;

import org.bson.BsonArray;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Splits a collection into ranges of the split key, with bounds taken from a sample of the
 * collection.
 *
 * <p>A MongoDB range filter such as {@code {key: {$lt: 5}}} only matches values of the type bracket
 * of its bound: numbers, strings, ObjectIds, dates and so on are compared only with values of their
 * own bracket. The splits therefore cover each bracket that the sampled bounds have with ranges of
 * that bracket alone, and one more split reads every document whose split key is missing, null or
 * of another type. Each document is read by exactly one split, unless its split key holds an array,
 * which a range filter matches by each element.
 */
public class SamplingSplitStrategy implements MongoSplitStrategy, Serializable {

    /**
     * The {@code $type} aliases of the type brackets that a range of the split key can cover. Null
     * and missing values, arrays and the other types are read by the split of other values.
     */
    private static final Map<BsonType, List<String>> RANGE_TYPE_BRACKETS = rangeTypeBrackets();

    /**
     * The {@code $type} aliases of every type that a split key value can have besides null and
     * arrays. {@code undefined} is listed because MongoDB 8.0 no longer matches it with null.
     */
    private static final List<String> ALL_TYPE_ALIASES =
            Arrays.asList(
                    "double",
                    "string",
                    "object",
                    "binData",
                    "undefined",
                    "objectId",
                    "bool",
                    "date",
                    "regex",
                    "dbPointer",
                    "javascript",
                    "symbol",
                    "javascriptWithScope",
                    "int",
                    "timestamp",
                    "long",
                    "decimal",
                    "minKey",
                    "maxKey");

    private final MongodbClientProvider clientProvider;

    private final String splitKey;

    private final BsonDocument matchQuery;

    private final BsonDocument projection;

    private final long samplesPerSplit;

    private final long sizePerSplit;

    SamplingSplitStrategy(
            MongodbClientProvider clientProvider,
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
        long count = numAndAvgSize.getLeft();
        long avgSize = numAndAvgSize.getRight();

        // Handle the case when avgSize is 0 to prevent division by zero
        if (avgSize <= 0) {
            // If there are documents in the collection, return a single split
            if (count > 0) {
                return Lists.newArrayList(
                        MongoSplitUtils.createMongoSplit(
                                0, matchQuery, projection, splitKey, null, null));
            } else {
                // If there are no documents, return an empty list
                return Lists.newArrayList();
            }
        }

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
        List<BsonDocument> samples = sampleCollection(numSamples);
        if (samples.isEmpty()) {
            // No sampled document has a split key value, so a range would not hold any of them
            return Lists.newArrayList(
                    MongoSplitUtils.createMongoSplit(
                            0, matchQuery, projection, splitKey, null, null));
        }

        List<BsonValue> rightBoundaries =
                IntStream.range(0, samples.size())
                        .filter(
                                i ->
                                        i % samplesPerSplit == 0
                                                || !matchQuery.isEmpty() && i == count - 1)
                        .mapToObj(i -> valueOf(samples.get(i), splitKey))
                        .filter(SamplingSplitStrategy::canBoundARange)
                        .collect(Collectors.toList());
        if (rightBoundaries.isEmpty()) {
            return Lists.newArrayList(
                    MongoSplitUtils.createMongoSplit(
                            0, matchQuery, projection, splitKey, null, null));
        }

        return createSplits(splitKey, rightBoundaries);
    }

    /** NaN sorts below all numbers but no range of numbers matches it, so it cannot be a bound. */
    private static boolean canBoundARange(BsonValue value) {
        if (!RANGE_TYPE_BRACKETS.containsKey(value.getBsonType())) {
            return false;
        }
        if (value.isDouble()) {
            return !Double.isNaN(value.asDouble().getValue());
        }
        return !value.isDecimal128() || !value.asDecimal128().getValue().isNaN();
    }

    /** Returns the value of {@code key}, or {@link BsonNull} when the document has none. */
    @VisibleForTesting
    static BsonValue valueOf(BsonDocument document, String key) {
        BsonValue value = document.get(key);
        return value == null || value instanceof BsonArray ? BsonNull.VALUE : value;
    }

    @VisibleForTesting
    protected ImmutablePair<Long, Long> getDocumentNumAndAvgSize() {
        String collectionName =
                clientProvider.getDefaultCollection().getNamespace().getCollectionName();
        BsonDocument statsCmd = new BsonDocument("collStats", new BsonString(collectionName));
        Document res = clientProvider.getDefaultDatabase().runCommand(statsCmd);
        Object count = res.get("count");
        // fix issue https://github.com/apache/seatunnel/issues/7575
        long total =
                Optional.ofNullable(count)
                        .map(v -> new BigDecimal(String.valueOf(count)).longValue())
                        .orElse(0L);
        Object avgDocumentBytes = res.get("avgObjSize");
        long avgObjSize =
                Optional.ofNullable(avgDocumentBytes)
                        .map(
                                docBytes -> {
                                    if (docBytes instanceof Integer) {
                                        return ((Integer) docBytes).longValue();
                                    } else if (docBytes instanceof Double) {
                                        return ((Double) docBytes).longValue();
                                    } else {
                                        return 0L;
                                    }
                                })
                        .orElse(0L);

        if (matchQuery == null || matchQuery.isEmpty()) {
            return ImmutablePair.of(total, avgObjSize);
        } else {
            return ImmutablePair.of(
                    clientProvider.getDefaultCollection().countDocuments(matchQuery), avgObjSize);
        }
    }

    /**
     * Samples documents that have a split key value, sorted by it. Documents whose split key is
     * missing or null cannot bound a range; the split of other values reads them.
     */
    @VisibleForTesting
    protected List<BsonDocument> sampleCollection(int numSamples) {
        BsonDocument hasSplitKey =
                new BsonDocument(splitKey, new BsonDocument("$ne", BsonNull.VALUE));
        return clientProvider
                .getDefaultCollection()
                .aggregate(
                        Lists.newArrayList(
                                Aggregates.match(MongoSplitUtils.combine(matchQuery, hasSplitKey)),
                                Aggregates.sample(numSamples),
                                Aggregates.project(Projections.include(splitKey)),
                                Aggregates.sort(Sorts.ascending(splitKey))))
                .allowDiskUse(true)
                .into(Lists.newArrayList());
    }

    /**
     * Creates the ranges of each type bracket of the sorted bounds, from below the first bound of
     * the bracket to above its last one, and then the split of all other values: missing, null,
     * NaN, arrays and the types that no bound has.
     */
    private List<MongoSplit> createSplits(String splitKey, List<BsonValue> rightBoundaries) {
        List<MongoSplit> splits = new ArrayList<>();
        Set<String> coveredTypes = new LinkedHashSet<>();
        int start = 0;
        while (start < rightBoundaries.size()) {
            List<String> bracket =
                    RANGE_TYPE_BRACKETS.get(rightBoundaries.get(start).getBsonType());
            int end = start;
            while (end + 1 < rightBoundaries.size()
                    && bracket.equals(
                            RANGE_TYPE_BRACKETS.get(rightBoundaries.get(end + 1).getBsonType()))) {
                end++;
            }
            splits.add(createSplit(splits.size(), null, rightBoundaries.get(start)));
            for (int i = start + 1; i <= end; i++) {
                splits.add(
                        createSplit(
                                splits.size(), rightBoundaries.get(i - 1), rightBoundaries.get(i)));
            }
            splits.add(createSplit(splits.size(), rightBoundaries.get(end), null));
            coveredTypes.addAll(bracket);
            start = end + 1;
        }
        splits.add(
                MongoSplitUtils.createMongoSplit(
                        splits.size(), matchQuery, projection, otherValues(coveredTypes)));
        return splits;
    }

    private MongoSplit createSplit(int index, BsonValue lowerBound, BsonValue upperBound) {
        return MongoSplitUtils.createMongoSplit(
                index, matchQuery, projection, splitKey, lowerBound, upperBound);
    }

    /**
     * A filter for the split key values that no range covers: missing and null ({@code {key: null}}
     * matches both), NaN, which no range of numbers matches, and the types of no range.
     */
    private BsonDocument otherValues(Set<String> coveredTypes) {
        List<BsonValue> filters = new ArrayList<>();
        filters.add(new BsonDocument(splitKey, BsonNull.VALUE));
        if (coveredTypes.contains("double")) {
            filters.add(new BsonDocument(splitKey, new BsonDouble(Double.NaN)));
            filters.add(new BsonDocument(splitKey, new BsonDecimal128(Decimal128.NaN)));
        }
        BsonArray otherTypes = new BsonArray();
        for (String alias : ALL_TYPE_ALIASES) {
            if (!coveredTypes.contains(alias)) {
                otherTypes.add(new BsonString(alias));
            }
        }
        if (!otherTypes.isEmpty()) {
            filters.add(new BsonDocument(splitKey, new BsonDocument("$type", otherTypes)));
        }
        return new BsonDocument("$or", new BsonArray(filters));
    }

    private static Map<BsonType, List<String>> rangeTypeBrackets() {
        Map<BsonType, List<String>> brackets = new EnumMap<>(BsonType.class);
        List<String> numbers = Arrays.asList("double", "int", "long", "decimal");
        brackets.put(BsonType.DOUBLE, numbers);
        brackets.put(BsonType.INT32, numbers);
        brackets.put(BsonType.INT64, numbers);
        brackets.put(BsonType.DECIMAL128, numbers);
        List<String> strings = Arrays.asList("string", "symbol");
        brackets.put(BsonType.STRING, strings);
        brackets.put(BsonType.SYMBOL, strings);
        brackets.put(BsonType.DOCUMENT, Collections.singletonList("object"));
        brackets.put(BsonType.BINARY, Collections.singletonList("binData"));
        brackets.put(BsonType.OBJECT_ID, Collections.singletonList("objectId"));
        brackets.put(BsonType.BOOLEAN, Collections.singletonList("bool"));
        brackets.put(BsonType.DATE_TIME, Collections.singletonList("date"));
        brackets.put(BsonType.TIMESTAMP, Collections.singletonList("timestamp"));
        return brackets;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private MongodbClientProvider clientProvider;

        private String splitKey;

        private BsonDocument matchQuery;

        private BsonDocument projection;

        private long samplesPerSplit;

        private long sizePerSplit;

        private static final BsonDocument EMPTY_MATCH_QUERY = new BsonDocument();

        private static final BsonDocument EMPTY_PROJECTION = new BsonDocument();

        private static final long DEFAULT_SAMPLES_PER_SPLIT = 10;

        Builder() {
            this.clientProvider = null;
            this.matchQuery = EMPTY_MATCH_QUERY;
            this.projection = EMPTY_PROJECTION;
            this.samplesPerSplit = DEFAULT_SAMPLES_PER_SPLIT;
        }

        public Builder setClientProvider(MongodbClientProvider clientProvider) {
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

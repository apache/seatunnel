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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

public class SamplingSplitStrategyTest {

    @Mock private MongodbClientProvider clientProvider;

    @Mock private MongoCollection<BsonDocument> collection;

    @Mock private MongoDatabase database;

    private SamplingSplitStrategy strategy;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        strategy = new SamplingSplitStrategy(clientProvider, "splitKey", null, null, 100L, 1000L);
        when(clientProvider.getDefaultCollection()).thenReturn(collection);
        when(clientProvider.getDefaultDatabase()).thenReturn(database);

        MongoNamespace namespace = new MongoNamespace("databaseName", "collectionName");
        when(collection.getNamespace()).thenReturn(namespace);
    }

    @Test
    public void testGetDocumentNumAndAvgSize() {
        BsonDocument statsCmd = new BsonDocument("collStats", new BsonString("collectionName"));
        Document res = new Document();
        res.put("count", "1.3360484963E10");
        res.put("avgObjSize", 200.0);

        when(database.runCommand(statsCmd)).thenReturn(res);

        ImmutablePair<Long, Long> result = strategy.getDocumentNumAndAvgSize();

        assertEquals(Long.valueOf(13360484963L), result.getLeft());
        assertEquals(Long.valueOf(200), result.getRight());
    }

    @Test
    public void testSplitWithZeroAvgSize() {
        // Mock the getDocumentNumAndAvgSize method to return zero avgSize
        SamplingSplitStrategy spyStrategy =
                new SamplingSplitStrategy(
                        clientProvider, "_id", new BsonDocument(), new BsonDocument(), 10L, 1024L) {
                    @Override
                    protected ImmutablePair<Long, Long> getDocumentNumAndAvgSize() {
                        return ImmutablePair.of(10L, 0L); // 10 documents with 0 avgSize
                    }
                };

        // This should not throw a division by zero exception
        java.util.List<MongoSplit> splits = spyStrategy.split();

        // Should return a single split when count > 0 and avgSize = 0
        assertEquals(1, splits.size());
    }

    @Test
    public void testSplitWithZeroAvgSizeAndZeroCount() {
        // Mock the getDocumentNumAndAvgSize method to return zero avgSize and zero count
        SamplingSplitStrategy spyStrategy =
                new SamplingSplitStrategy(
                        clientProvider, "_id", new BsonDocument(), new BsonDocument(), 10L, 1024L) {
                    @Override
                    protected ImmutablePair<Long, Long> getDocumentNumAndAvgSize() {
                        return ImmutablePair.of(0L, 0L); // 0 documents with 0 avgSize
                    }
                };

        // This should not throw a division by zero exception
        java.util.List<MongoSplit> splits = spyStrategy.split();

        // Should return an empty list when count = 0 and avgSize = 0
        assertEquals(0, splits.size());
    }

    private static final ObjectId OID_1 = new ObjectId("6afdcddc61af284521ed3ff8");
    private static final ObjectId OID_2 = new ObjectId("6afdcddc61af284521ed4105");

    /**
     * A strategy over 3000 documents of 200 bytes, which 64 KB splits cut into 10, whose sample is
     * {@code sampledKeys} in this order and where every sampled document bounds a split.
     */
    private SamplingSplitStrategy strategyWithSample(
            String splitKey, BsonDocument matchQuery, BsonValue... sampledKeys) {
        return new SamplingSplitStrategy(
                clientProvider, splitKey, matchQuery, new BsonDocument(), 1L, 64 * 1024L) {
            @Override
            protected ImmutablePair<Long, Long> getDocumentNumAndAvgSize() {
                return ImmutablePair.of(3000L, 200L);
            }

            @Override
            protected List<BsonDocument> sampleCollection(int numSamples) {
                List<BsonDocument> samples = new ArrayList<>();
                for (BsonValue key : sampledKeys) {
                    samples.add(new BsonDocument(splitKey, key));
                }
                return samples;
            }
        };
    }

    private static List<String> queries(List<MongoSplit> splits) {
        return splits.stream().map(s -> s.getQuery().toJson()).collect(Collectors.toList());
    }

    /**
     * The sample of an _id holding numbers, strings and ObjectIds. A range whose bounds have two
     * types matches no document, so every type gets its own ranges, and the last split reads the
     * values that no range covers.
     */
    @Test
    public void testSplitsCoverEachTypeOfTheSampledBoundsWithItsOwnRanges() {
        List<MongoSplit> splits =
                strategyWithSample(
                                "_id",
                                new BsonDocument(),
                                new BsonInt32(12),
                                new BsonInt32(240),
                                new BsonString("s0155"),
                                new BsonObjectId(OID_1),
                                new BsonObjectId(OID_2))
                        .split();

        assertEquals(
                Arrays.asList(
                        "{\"_id\": {\"$lt\": 12}}",
                        "{\"$and\": [{\"_id\": {\"$gte\": 12}}, {\"_id\": {\"$lt\": 240}}]}",
                        "{\"_id\": {\"$gte\": 240}}",
                        "{\"_id\": {\"$lt\": \"s0155\"}}",
                        "{\"_id\": {\"$gte\": \"s0155\"}}",
                        "{\"_id\": {\"$lt\": {\"$oid\": \"6afdcddc61af284521ed3ff8\"}}}",
                        "{\"$and\": [{\"_id\": {\"$gte\": {\"$oid\": \"6afdcddc61af284521ed3ff8\"}}},"
                                + " {\"_id\": {\"$lt\": {\"$oid\": \"6afdcddc61af284521ed4105\"}}}]}",
                        "{\"_id\": {\"$gte\": {\"$oid\": \"6afdcddc61af284521ed4105\"}}}",
                        "{\"$or\": [{\"_id\": null}, {\"_id\": {\"$numberDouble\": \"NaN\"}},"
                                + " {\"_id\": {\"$numberDecimal\": \"NaN\"}},"
                                + " {\"_id\": {\"$type\": [\"object\", \"binData\", \"undefined\", \"bool\","
                                + " \"date\", \"regex\", \"dbPointer\", \"javascript\","
                                + " \"javascriptWithScope\", \"timestamp\", \"minKey\","
                                + " \"maxKey\"]}}]}"),
                queries(splits));
        assertEquals("split-8", splits.get(8).splitId());
    }

    /**
     * A document without the split key used to bound no side of a split, which then read the whole
     * collection. Missing, null and NaN values now bound nothing and are read by the last split.
     */
    @Test
    public void testMissingNullAndNaNKeysBoundNoSplit() {
        List<MongoSplit> splits =
                strategyWithSample(
                                "code",
                                new BsonDocument(),
                                new BsonDouble(Double.NaN),
                                new BsonInt32(248),
                                new BsonInt32(675))
                        .split();

        assertEquals(
                Arrays.asList(
                        "{\"code\": {\"$lt\": 248}}",
                        "{\"$and\": [{\"code\": {\"$gte\": 248}}, {\"code\": {\"$lt\": 675}}]}",
                        "{\"code\": {\"$gte\": 675}}",
                        "{\"$or\": [{\"code\": null}, {\"code\": {\"$numberDouble\": \"NaN\"}},"
                                + " {\"code\": {\"$numberDecimal\": \"NaN\"}},"
                                + " {\"code\": {\"$type\": [\"string\", \"object\", \"binData\","
                                + " \"undefined\", \"objectId\", \"bool\", \"date\", \"regex\", \"dbPointer\","
                                + " \"javascript\", \"symbol\", \"javascriptWithScope\","
                                + " \"timestamp\", \"minKey\", \"maxKey\"]}}]}"),
                queries(splits));
    }

    /** When no sampled document has a split key value, one split reads the whole collection. */
    @Test
    public void testNoSampledSplitKeyValueGivesOneSplit() {
        List<MongoSplit> splits =
                strategyWithSample("code", BsonDocument.parse("{\"a\": 1}")).split();

        assertEquals(Arrays.asList("{\"a\": 1}"), queries(splits));
    }

    /**
     * A match query on the split key used to be replaced by the bound of the first and the last
     * split, which then read documents that the match query excludes.
     */
    @Test
    public void testEverySplitKeepsTheMatchQuery() {
        BsonDocument matchQuery = BsonDocument.parse("{\"_id\": {\"$gte\": 1500}}");
        List<MongoSplit> splits =
                strategyWithSample("_id", matchQuery, new BsonInt32(1509), new BsonInt32(1808))
                        .split();

        assertEquals(4, splits.size());
        for (MongoSplit split : splits) {
            List<BsonValue> conditions = split.getQuery().getArray("$and").getValues();
            assertEquals(2, conditions.size(), split.getQuery().toJson());
            assertEquals(matchQuery, conditions.get(0));
        }
        assertEquals(
                "{\"_id\": {\"$lt\": 1509}}",
                splits.get(0).getQuery().getArray("$and").get(1).asDocument().toJson());
    }

    /**
     * When no sampled value can bound a range, one split reads the whole collection: a split of
     * other values alone would not read an empty array.
     */
    @Test
    public void testNoValueThatCanBoundARangeGivesOneSplit() {
        List<MongoSplit> splits =
                strategyWithSample(
                                "code",
                                new BsonDocument(),
                                new BsonDouble(Double.NaN),
                                new BsonRegularExpression("a"))
                        .split();

        assertEquals(Arrays.asList("{}"), queries(splits));
    }

    /** An array cannot bound a range, so it reads as no value. */
    @Test
    public void testValueOfIgnoresArrays() {
        BsonDocument document = BsonDocument.parse("{\"list\": [1, 2], \"c\": 3}");

        assertEquals(new BsonInt32(3), SamplingSplitStrategy.valueOf(document, "c"));
        assertEquals(BsonNull.VALUE, SamplingSplitStrategy.valueOf(document, "missing"));
        assertEquals(BsonNull.VALUE, SamplingSplitStrategy.valueOf(document, "list"));
    }
}

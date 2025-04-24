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

import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;

import org.apache.commons.lang3.tuple.ImmutablePair;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

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
}

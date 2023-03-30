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

import org.bson.BsonDocument;

import javax.annotation.Nullable;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;

/** Helper class for using {@link MongoSplit}. */
public class MongoSplitUtils {

    private static final String SPLIT_ID_TEMPLATE = "split-%d";

    public static MongoSplit createMongoSplit(
            int index,
            BsonDocument matchQuery,
            BsonDocument projection,
            String splitKey,
            @Nullable Object lowerBound,
            @Nullable Object upperBound) {
        return createMongoSplit(index, matchQuery, projection, splitKey, lowerBound, upperBound, 0);
    }

    public static MongoSplit createMongoSplit(
            int index,
            BsonDocument matchQuery,
            BsonDocument projection,
            String splitKey,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            long startOffset) {
        BsonDocument splitQuery = new BsonDocument();
        if (matchQuery != null) {
            matchQuery.forEach(splitQuery::append);
        }
        if (splitKey != null) {
            BsonDocument boundaryQuery;
            if (lowerBound != null && upperBound != null) {
                boundaryQuery =
                        and(gte(splitKey, lowerBound), lt(splitKey, upperBound)).toBsonDocument();
            } else if (lowerBound != null) {
                boundaryQuery = gte(splitKey, lowerBound).toBsonDocument();
            } else if (upperBound != null) {
                boundaryQuery = lt(splitKey, upperBound).toBsonDocument();
            } else {
                boundaryQuery = new BsonDocument();
            }
            boundaryQuery.forEach(splitQuery::append);
        }
        return new MongoSplit(
                String.format(SPLIT_ID_TEMPLATE, index), splitQuery, projection, startOffset);
    }
}

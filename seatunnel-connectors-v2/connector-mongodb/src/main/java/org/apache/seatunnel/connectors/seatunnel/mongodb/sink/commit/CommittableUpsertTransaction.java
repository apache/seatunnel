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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CommittableUpsertTransaction extends CommittableTransaction {

    private final Function<BsonDocument, BsonDocument> filterConditions;

    public CommittableUpsertTransaction(
            MongoCollection<BsonDocument> collection,
            List<BsonDocument> documents,
            Function<BsonDocument, BsonDocument> filterConditions) {
        super(collection, documents);
        this.filterConditions = filterConditions;
    }

    @Override
    public Integer execute() {
        List<UpdateOneModel<BsonDocument>> updateOneModelList =
                bufferedDocuments.stream()
                        .map(
                                document -> {
                                    Bson filter =
                                            generateFilter(
                                                    filterConditions.apply(
                                                            document.getDocument("$set")));
                                    document.remove("_id");
                                    return new UpdateOneModel<BsonDocument>(
                                            filter, document, new UpdateOptions().upsert(true));
                                })
                        .collect(Collectors.toList());
        BulkWriteResult bulkWriteResult = collection.bulkWrite(updateOneModelList);
        return bulkWriteResult.getUpserts().size() + bulkWriteResult.getInsertedCount();
    }

    public static Bson generateFilter(BsonDocument filterConditions) {
        List<Bson> filters =
                filterConditions.entrySet().stream()
                        .map(entry -> Filters.eq(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

        return Filters.and(filters);
    }
}

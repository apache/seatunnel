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

package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbWriterOptions;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RowDataDocumentSerializer implements DocumentSerializer<SeaTunnelRow> {

    private final RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter;

    private final Boolean isUpsertEnable;

    private final Function<BsonDocument, BsonDocument> filterConditions;

    public RowDataDocumentSerializer(
            RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter,
            MongodbWriterOptions options,
            Function<BsonDocument, BsonDocument> filterConditions) {
        this.rowDataToBsonConverter = rowDataToBsonConverter;
        this.isUpsertEnable = options.isUpsertEnable();
        this.filterConditions = filterConditions;
    }

    @Override
    public WriteModel<BsonDocument> serializeToWriteModel(SeaTunnelRow row) {
        final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
        if (isUpsertEnable) {
            Bson filter = generateFilter(filterConditions.apply(bsonDocument));
            bsonDocument.remove("_id");
            BsonDocument update = new BsonDocument("$set", bsonDocument);
            return new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
        } else {
            return new InsertOneModel<>(bsonDocument);
        }
    }

    public static Bson generateFilter(BsonDocument filterConditions) {
        List<Bson> filters =
                filterConditions.entrySet().stream()
                        .map(entry -> Filters.eq(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

        return Filters.and(filters);
    }
}

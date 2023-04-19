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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.SerializableFunction;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config.MongodbWriterOptions;

import org.bson.BsonDocument;
import org.bson.BsonValue;

public class MongoKeyExtractor implements SerializableFunction<BsonDocument, BsonDocument> {

    private static final long serialVersionUID = 1L;

    private static String[] upsertKey;

    public MongoKeyExtractor(MongodbWriterOptions options) {
        upsertKey = options.getUpsertKey();
    }

    @Override
    public BsonDocument apply(BsonDocument bsonDocument) {
        BsonDocument filterConditions = new BsonDocument();
        for (String key : upsertKey) {
            BsonValue bsonValue = bsonDocument.get(key);
            filterConditions.append(key, bsonValue);
        }
        return filterConditions;
    }
}

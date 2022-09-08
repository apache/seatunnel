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

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbParameters;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.IOException;

public class MongodbSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SeaTunnelRowType rowType;

    private final MongodbParameters params;

    private final SerializationSchema serializationSchema;

    private MongoClient client;

    public MongodbSinkWriter(SeaTunnelRowType rowType, MongodbParameters params) {
        this.rowType = rowType;
        this.params = params;
        // TODO according to format to initialize serializationSchema
        // Now temporary using json serializationSchema
        this.serializationSchema = new JsonSerializationSchema(rowType);
    }

    @Override
    public void write(SeaTunnelRow rows) throws IOException {
        byte[] serialize = serializationSchema.serialize(rows);
        String content = new String(serialize);

        String database = this.params.getDatabase();
        String collection = this.params.getCollection();
        this.client = MongoClients.create(params.getUri());
        MongoCollection<Document> mongoCollection = this.client
            .getDatabase(database)
            .getCollection(collection);

        Document doc = Document.parse(content);
        mongoCollection.insertOne(doc);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}

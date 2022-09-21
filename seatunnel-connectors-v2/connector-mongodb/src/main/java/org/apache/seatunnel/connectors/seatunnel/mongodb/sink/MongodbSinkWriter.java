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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbParameters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.data.DefaultSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.data.Serializer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.IOException;

public class MongodbSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SeaTunnelRowType rowType;

    private final Serializer serializer;

    private final MongoClient client;

    private final String database;

    private final String collection;

    private final MongoCollection<Document> mongoCollection;

    private final boolean useSimpleTextSchema;

    public MongodbSinkWriter(SeaTunnelRowType rowType,
                             boolean useSimpleTextSchema,
                             MongodbParameters params) {
        this.rowType = rowType;
        this.database = params.getDatabase();
        this.collection = params.getCollection();
        this.client = MongoClients.create(params.getUri());
        this.mongoCollection = this.client.getDatabase(database).getCollection(collection);
        this.useSimpleTextSchema = useSimpleTextSchema;
        this.serializer = useSimpleTextSchema ? null : new DefaultSerializer(rowType);
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        Document document;
        if (useSimpleTextSchema) {
            String simpleText = row.getField(0).toString();
            document = Document.parse(simpleText);
        } else {
            document = serializer.serialize(row);
        }
        mongoCollection.insertOne(document);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}

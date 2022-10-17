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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbParameters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.data.DefaultDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.data.Deserializer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;

@Slf4j
public class MongodbSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final SingleSplitReaderContext context;

    private MongoClient client;

    private final MongodbParameters params;

    private final Deserializer deserializer;

    private final Bson projectionFields;

    private final boolean useSimpleTextSchema;

    MongodbSourceReader(SingleSplitReaderContext context,
                        MongodbParameters params,
                        SeaTunnelRowType rowType,
                        boolean useSimpleTextSchema) {
        this.context = context;
        this.params = params;
        this.useSimpleTextSchema = useSimpleTextSchema;
        if (useSimpleTextSchema) {
            this.deserializer = null;
            this.projectionFields = null;
        } else {
            this.deserializer = new DefaultDeserializer(rowType);
            this.projectionFields = Projections.fields(
                Projections.include(rowType.getFieldNames()),
                Projections.excludeId());
        }
    }

    @Override
    public void open() throws Exception {
        client = MongoClients.create(params.getUri());
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try (MongoCursor<Document> mongoCursor = client.getDatabase(params.getDatabase())
            .getCollection(params.getCollection())
            .find()
            .projection(projectionFields)
            .iterator()) {
            while (mongoCursor.hasNext()) {
                Document document = mongoCursor.next();
                if (useSimpleTextSchema) {
                    output.collect(new SeaTunnelRow(new Object[]{document.toJson()}));
                } else {
                    output.collect(deserializer.deserialize(document));
                }
            }
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded mongodb source");
                context.signalNoMoreElement();
            }
        }
    }

}

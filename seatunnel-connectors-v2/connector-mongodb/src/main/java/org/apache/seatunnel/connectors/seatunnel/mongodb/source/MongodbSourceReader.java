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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbParameters;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MongodbSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongodbSourceReader.class);

    private final SingleSplitReaderContext context;

    private MongoClient client;

    private final MongodbParameters params;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    MongodbSourceReader(SingleSplitReaderContext context, MongodbParameters params, DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.context = context;
        this.params = params;
        this.deserializationSchema = deserializationSchema;
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
        try (MongoCursor<Document> mongoCursor = client.getDatabase(params.getDatabase()).getCollection(params.getCollection()).find().iterator()) {

            while (mongoCursor.hasNext()) {
                Document doc = mongoCursor.next();
                HashMap<String, Object> map = new HashMap<>(doc.size());
                Set<Map.Entry<String, Object>> entries = doc.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    if (!"_id".equalsIgnoreCase(entry.getKey())) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        map.put(key, value);
                    }
                }
                String content = JsonUtils.toJsonString(map);
                if (deserializationSchema != null) {
                    deserializationSchema.deserialize(content.getBytes(), output);
                } else {
                    // TODO: use seatunnel-text-format
                    output.collect(new SeaTunnelRow(new Object[]{content}));
                }
            }
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                LOGGER.info("Closed the bounded mongodb source");
                context.signalNoMoreElement();
            }
        }
    }

}

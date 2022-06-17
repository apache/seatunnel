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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongodbSourceReader implements SourceReader<SeaTunnelRow, MongodbSourceSplit> {

    private final SourceReader.Context context;

    private MongoClient client;

    private final MongodbSourceEvent params;

    private final SeaTunnelRowType rowTypeInfo;

    private final List<MongodbSourceSplit> splits;

    MongodbSourceReader(SourceReader.Context context, MongodbSourceEvent params, SeaTunnelRowType rowTypeInfo) {
        this.context = context;
        this.params = params;
        this.rowTypeInfo = rowTypeInfo;
        this.splits = new ArrayList<>();
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
        if (!splits.isEmpty()) {
            try (MongoCursor<Document> mongoCursor = client
                    .getDatabase(params.getDatabase())
                    .getCollection(params.getCollection())
                    .find()
                    .iterator()) {

                while (mongoCursor.hasNext()) {
                    Document doc = mongoCursor.next();
                    Object[] values = new Object[this.rowTypeInfo.getFieldNames().length];
                    Set<Map.Entry<String, Object>> entries = doc.entrySet();
                    int i = 0;
                    for (Map.Entry<String, Object> entry : entries) {
                        if (!"_id".equalsIgnoreCase(entry.getKey())) {
                            values[i] = entry.getValue();
                            i++;
                        }
                    }
                    output.collect(new SeaTunnelRow(values));
                }
            } finally {
                // signal to the source that we have reached the end of the data.
                this.context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List<MongodbSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<MongodbSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}

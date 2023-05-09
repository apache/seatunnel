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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.config.MongodbReadOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;

import org.bson.BsonDocument;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/** MongoReader reads MongoDB by splits (queries). */
@Slf4j
public class MongodbReader implements SourceReader<SeaTunnelRow, MongoSplit> {

    private final Queue<MongoSplit> pendingSplits;

    private final DocumentDeserializer<SeaTunnelRow> deserializer;

    private final SourceReader.Context context;

    private final MongodbClientProvider clientProvider;

    private MongoCursor<BsonDocument> cursor;

    private final MongodbReadOptions readOptions;

    private volatile boolean noMoreSplit;

    public MongodbReader(
            SourceReader.Context context,
            MongodbClientProvider clientProvider,
            DocumentDeserializer<SeaTunnelRow> deserializer,
            MongodbReadOptions mongodbReadOptions) {
        this.deserializer = deserializer;
        this.context = context;
        this.clientProvider = clientProvider;
        pendingSplits = new ConcurrentLinkedDeque<>();
        this.readOptions = mongodbReadOptions;
    }

    @Override
    public void open() throws Exception {
        if (cursor != null) {
            cursor.close();
        }
    }

    @Override
    public void close() throws IOException {
        if (cursor != null) {
            cursor.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            MongoSplit currentSplit = pendingSplits.poll();
            if (null != currentSplit) {
                if (cursor != null) {
                    // current split is in-progress
                    return;
                }
                log.info("Prepared to read split {}", currentSplit.splitId());
                FindIterable<BsonDocument> rs =
                        clientProvider
                                .getDefaultCollection()
                                .find(currentSplit.getQuery())
                                .projection(currentSplit.getProjection())
                                .batchSize(readOptions.getFetchSize())
                                .noCursorTimeout(readOptions.isNoCursorTimeout())
                                .maxTime(readOptions.getMaxTimeMS(), TimeUnit.MINUTES);
                cursor = rs.iterator();
                while (cursor.hasNext()) {
                    SeaTunnelRow deserialize = deserializer.deserialize(cursor.next());
                    output.collect(deserialize);
                }
                closeCurrentSplit();
            }
            if (noMoreSplit && pendingSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded mongodb source");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List<MongoSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<MongoSplit> splits) {
        log.info("Adding split(s) to reader: {}", splits);
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this reader will not add new split.");
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void closeCurrentSplit() {
        Preconditions.checkNotNull(cursor);
        cursor.close();
        cursor = null;
    }
}

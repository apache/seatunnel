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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBConfig;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.serialize.DocumentDBItemDeserializer;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class AmazonDocumentDBSourceReader
        implements SourceReader<SeaTunnelRow, AmazonDocumentDBSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonDocumentDBSourceReader.class);

    private final Context context;
    private final AmazonDocumentDBConfig config;
    private final DocumentDBItemDeserializer deserializer;
    private final Queue<AmazonDocumentDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();

    private MongoClient client;
    private MongoCollection<BsonDocument> collection;
    private MongoCursor<BsonDocument> cursor;
    private AmazonDocumentDBSourceSplit currentSplit;
    private volatile boolean noMoreSplits;
    private volatile boolean finished;

    public AmazonDocumentDBSourceReader(
            Context context, AmazonDocumentDBConfig config, SeaTunnelRowType rowType) {
        this.context = context;
        this.config = config;
        this.deserializer = new DocumentDBItemDeserializer(rowType);
    }

    @Override
    public void open() {
        try {
            client = createMongoClient();
            collection =
                    client.getDatabase(config.getDatabase())
                            .getCollection(config.getCollection(), BsonDocument.class);
        } catch (Exception e) {
            close();
            throw new IllegalStateException(
                    String.format(
                            "Failed to open AmazonDocumentDB source reader for database [%s], collection [%s]",
                            config.getDatabase(), config.getCollection()),
                    e);
        }
    }

    @Override
    public void close() {
        closeCursor();
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        AmazonDocumentDBSourceSplit activeSplit;
        synchronized (output.getCheckpointLock()) {
            if (finished) {
                return;
            }
            if (currentSplit == null) {
                currentSplit = pendingSplits.poll();
            }
            if (currentSplit == null) {
                finishReaderIfNoMoreWork();
                return;
            }
            activeSplit = currentSplit;
        }

        BsonDocument document = fetchNextDocument(activeSplit);

        synchronized (output.getCheckpointLock()) {
            if (finished || currentSplit != activeSplit) {
                return;
            }
            if (document == null) {
                finishCurrentSplit();
                finishReaderIfNoMoreWork();
                return;
            }
            output.collect(deserializer.deserialize(document));
        }
    }

    @Override
    public List<AmazonDocumentDBSourceSplit> snapshotState(long checkpointId) {
        List<AmazonDocumentDBSourceSplit> state = new ArrayList<>();
        pendingSplits.forEach(split -> state.add(split.copy()));
        if (currentSplit != null) {
            state.add(currentSplit.copy());
        }
        return state;
    }

    @Override
    public void addSplits(List<AmazonDocumentDBSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no-op
    }

    MongoClient createMongoClient() {
        return MongoClients.create(config.createMongoClientSettings());
    }

    BsonDocument fetchNextDocument(AmazonDocumentDBSourceSplit split) {
        try {
            if (cursor == null) {
                FindIterable<BsonDocument> findIterable =
                        collection.find(BsonDocument.parse(split.getMatchQuery()));
                if (split.getProjection() != null) {
                    findIterable.projection(BsonDocument.parse(split.getProjection()));
                }
                cursor = findIterable.batchSize(config.getFetchSize()).iterator();
            }
            if (cursor.hasNext()) {
                return cursor.next();
            }
            closeCursor();
            return null;
        } catch (Exception e) {
            closeCursor();
            throw new IllegalStateException(
                    String.format(
                            "Failed to read AmazonDocumentDB data from database [%s], collection [%s]",
                            config.getDatabase(), config.getCollection()),
                    e);
        }
    }

    private void closeCursor() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    private void finishCurrentSplit() {
        LOG.info("AmazonDocumentDB reader [{}] finished source scan", context.getIndexOfSubtask());
        currentSplit = null;
    }

    private void finishReaderIfNoMoreWork() {
        if (currentSplit == null && pendingSplits.isEmpty() && noMoreSplits) {
            context.signalNoMoreElement();
            finished = true;
        }
    }

    int getFetchSize() {
        return config.getFetchSize();
    }
}

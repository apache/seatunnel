package org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;

import org.bson.Document;

import com.google.common.base.Preconditions;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/** MongoReader reads MongoDB by splits (queries). */
@Slf4j
public class MongoReader implements SourceReader<SeaTunnelRow, MongoSplit> {

    private final Queue<MongoSplit> pendingSplits;
    private final DocumentDeserializer<SeaTunnelRow> deserializer;
    private final SourceReader.Context context;
    private final MongoClientProvider clientProvider;

    private transient MongoCursor<Document> cursor;

    private MongoSplit currentSplit;

    private static final int DEFAULT_FETCH_SIZE = 200;

    public MongoReader(
            SourceReader.Context context,
            MongoClientProvider clientProvider,
            DocumentDeserializer<SeaTunnelRow> deserializer) {
        this.deserializer = deserializer;
        this.context = context;
        this.clientProvider = clientProvider;
        pendingSplits = new LinkedBlockingQueue<>();
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
                FindIterable<Document> rs =
                        clientProvider
                                .getDefaultCollection()
                                .find(currentSplit.getQuery())
                                .projection(currentSplit.getProjection())
                                .batchSize(DEFAULT_FETCH_SIZE);
                cursor = rs.iterator();
                while (cursor.hasNext()) {
                    SeaTunnelRow deserialize = deserializer.deserialize(cursor.next());
                    output.collect(deserialize);
                }
                if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                    closeCurrentSplit();
                    // signal to the source that we have reached the end of the data.
                    log.info("Closed the bounded mongodb source");
                    context.signalNoMoreElement();
                }
            }
        }
    }

    @Override
    public List<MongoSplit> snapshotState(long checkpointId) throws Exception {
        System.out.println("aaa");
        return null;
    }

    @Override
    public void addSplits(List<MongoSplit> splits) {
        log.info("Adding split(s) to reader: {}", splits);
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this reader will not add new split.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void closeCurrentSplit() {
        Preconditions.checkNotNull(cursor);
        cursor.close();
        cursor = null;
    }
}

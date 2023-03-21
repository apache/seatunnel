package org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.bson.Document;


import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * MongoReader reads MongoDB by splits (queries).
 **/
@Slf4j
public class MongoReader implements SourceReader<SeaTunnelRow, MongoSplit> {

    private final Queue<MongoSplit> pendingSplits;
    private final DocumentDeserializer<SeaTunnelRow> deserializer;
    private  SourceReader.Context context;
    private  MongoClientProvider clientProvider;

    private transient MongoCursor<Document> cursor;

    private MongoSplit currentSplit;

    private int offset = 0;

    private int fetchSize = DEFAULT_FETCH_SIZE;

    private static final int DEFAULT_FETCH_SIZE = 200;

    public MongoReader(SourceReader.Context context,
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
        System.out.printf("close");

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            MongoSplit currentSplit = pendingSplits.poll();
            if (null != currentSplit){
                prepareRead(currentSplit);
                Preconditions.checkNotNull(currentSplit);
                Preconditions.checkNotNull(cursor);

                List<Document> documents = Lists.newArrayList();
                while (documents.size() < fetchSize && cursor.hasNext()) {
                    SeaTunnelRow deserialize = deserializer.deserialize(cursor.next());
                    output.collect(deserialize);
                }
                offset += documents.size();
                if (cursor.hasNext()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Fetched {} records from split {}, current offset: {}", documents.size(), currentSplit, offset);
                    }

                } else {
                    String splitId = currentSplit.splitId();
                    //closeCurrentSplit();
                    //return MongoRecords.finishedSplit(splitId, documents);
                }
            }else {
                log.info("No more splits can be read.");
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
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private void prepareRead(MongoSplit currentSplit) throws IOException {
        if (cursor != null) {
            // current split is in-progress
            return;
        }
        log.info("Prepared to read split {}", currentSplit.splitId());
        offset = 0;
        FindIterable<Document> rs =
                clientProvider.getDefaultCollection()
                        .find(currentSplit.getQuery())
                        .projection(currentSplit.getProjection())
                        .batchSize(fetchSize);
        cursor = rs.iterator();

    }

    private void closeCurrentSplit() {
        Preconditions.checkNotNull(currentSplit);
        log.info("Finished reading split {}.", currentSplit.splitId());
        currentSplit = null;

        Preconditions.checkNotNull(cursor);
        cursor.close();
        cursor = null;
    }
}

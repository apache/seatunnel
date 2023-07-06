/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink;

import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StreamLoadEntityMeta;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StreamLoadResponse;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StreamLoadDataFormat;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class TransactionTableRegion implements TableRegion {

    enum State {
        ACTIVE,
        FLUSHING
    }

    private final StreamLoadManager manager;
    private final StreamLoader streamLoader;
    private final String database;
    private final String table;
    private final StreamLoadDataFormat dataFormat;
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong flushBytes = new AtomicLong();
    private final AtomicLong flushRows = new AtomicLong();

    private final AtomicReference<State> state;
    private final AtomicBoolean ctl = new AtomicBoolean(false);

    private volatile Queue<byte[]> outBuffer = new LinkedList<>();
    private volatile Queue<byte[]> inBuffer;
    private volatile StreamLoadEntityMeta entityMeta;
    private volatile String label;
    private volatile Future<?> responseFuture;
    private volatile long lastCommitTimeMills;
    private SinkConfig sinkConfig;
    private volatile boolean flushing;

    public TransactionTableRegion(
            StreamLoadManager manager,
            SinkConfig sinkConfig,
            TransactionStreamLoader streamLoader) {
        this.database = sinkConfig.getDatabase();
        this.table = sinkConfig.getTable();
        this.manager = manager;
        this.sinkConfig = sinkConfig;
        this.dataFormat =
                sinkConfig.getLoadFormat() == SinkConfig.StreamLoadFormat.JSON
                        ? StreamLoadDataFormat.JSON
                        : StreamLoadDataFormat.CSV;
        this.streamLoader = streamLoader;
        this.state = new AtomicReference<>(State.ACTIVE);
        this.lastCommitTimeMills = System.currentTimeMillis();
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public long getCacheBytes() {
        return cacheBytes.get();
    }

    @Override
    public StreamLoadEntityMeta getEntityMeta() {
        return entityMeta;
    }

    @Override
    public int write(byte[] row) {
        if (row == null) {
            return 0;
        }

        int c;
        if (ctl.compareAndSet(false, true)) {
            c = write0(row);
        } else {
            for (; ; ) {
                if (ctl.compareAndSet(false, true)) {
                    c = write0(row);
                    break;
                }
            }
        }
        ctl.set(false);
        return c;
    }

    protected int write0(byte[] row) {
        if (outBuffer == null) {
            outBuffer = new LinkedList<>();
        }
        outBuffer.offer(row);
        cacheBytes.addAndGet(row.length);
        return row.length;
    }

    @Override
    public boolean isFlushing() {
        return state.get() == State.FLUSHING;
    }

    @Override
    public boolean flush() {
        if (state.compareAndSet(State.ACTIVE, State.FLUSHING)) {
            for (; ; ) {
                if (ctl.compareAndSet(false, true)) {
                    log.info("flushing region label : {}, bytes : {}", label, cacheBytes.get());
                    inBuffer = outBuffer;
                    outBuffer = null;
                    ctl.set(false);
                    break;
                }
            }
            if (inBuffer != null && !inBuffer.isEmpty()) {
                streamLoad();
                return true;
            } else {
                state.compareAndSet(State.FLUSHING, State.ACTIVE);
                return false;
            }
        } else {
            log.info(
                    "can not flush region label : {}, bytes : {}, the region is flushing now",
                    label,
                    cacheBytes.get());
        }
        return false;
    }

    @Override
    public void callback(StreamLoadResponse response) {
        manager.callback(response);
    }

    @Override
    public void callback(Throwable e) {
        manager.callback(e);
    }

    @Override
    public void complete(StreamLoadResponse response) {
        response.setFlushBytes(flushBytes.get());
        response.setFlushRows(flushRows.get());
        callback(response);

        log.info("Stream load flushed, db: {}, table: {}, label : {}", database, table, label);
        if (!inBuffer.isEmpty()) {
            log.info("Stream load continue, db: {}, table: {}, label : {}", database, table, label);
            streamLoad();
            return;
        }
        if (state.compareAndSet(State.FLUSHING, State.ACTIVE)) {
            log.info(
                    "Stream load completed, db: {}, table: {}, label : {}, region cache bytes : {}",
                    database,
                    table,
                    label,
                    cacheBytes.get());
        }
    }

    @Override
    public void setResult(Future<?> result) {
        responseFuture = result;
    }

    @Override
    public Future<?> getResult() {
        return responseFuture;
    }

    protected void flip() {
        flushBytes.set(0L);
        flushRows.set(0L);
        responseFuture = null;

        StreamLoadEntityMeta chunkMeta = genEntityMeta();
        this.entityMeta = chunkMeta;
        log.info(
                "Generate entity meta, db: {}, table: {}, total rows : {}, entity rows : {}, entity bytes : {}",
                database,
                table,
                inBuffer.size(),
                chunkMeta.getRows(),
                chunkMeta.getBytes());
    }

    protected void streamLoad() {
        try {
            flip();
            setResult(streamLoader.send(this));
        } catch (Exception e) {
            callback(e);
        }
    }

    protected StreamLoadEntityMeta genEntityMeta() {
        long chunkBytes = 0;
        long chunkRows = 0;

        int delimiter = dataFormat.delimiter() == null ? 0 : dataFormat.delimiter().length;
        if (dataFormat.first() != null) {
            chunkBytes += dataFormat.first().length;
        }
        if (dataFormat.end() != null) {
            chunkBytes += dataFormat.end().length;
        }

        boolean first = true;
        for (byte[] bytes : inBuffer) {
            int d = first ? 0 : delimiter;
            first = false;
            if (chunkBytes + d + bytes.length > sinkConfig.getSinkChunkLimit()) {
                break;
            }
            chunkBytes += bytes.length + d;
            chunkRows++;
        }

        return new StreamLoadEntityMeta(chunkBytes, chunkRows);
    }

    public byte[] read() {
        if (flushRows.get() == entityMeta.getRows()) {
            flushing = false;
            return null;
        }

        byte[] row = inBuffer.poll();

        if (row == null) {
            flushing = false;
            return null;
        }

        if (!flushing) {
            flushing = true;
        }
        cacheBytes.addAndGet(-row.length);
        flushBytes.addAndGet(row.length);
        flushRows.incrementAndGet();
        return row;
    }

    @Override
    public StreamLoadDataFormat getDataFormat() {
        return dataFormat;
    }
}

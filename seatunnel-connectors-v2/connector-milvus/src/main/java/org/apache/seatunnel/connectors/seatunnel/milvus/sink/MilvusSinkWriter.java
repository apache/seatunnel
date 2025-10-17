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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.state.MilvusCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.milvus.state.MilvusSinkState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/** MilvusSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Milvus. */
@Slf4j
public class MilvusSinkWriter
        implements SinkWriter<SeaTunnelRow, MilvusCommitInfo, MilvusSinkState> {

    private final MilvusBufferBatchWriter batchWriter;
    private ReadonlyConfig config;

    private final ScheduledExecutorService scheduler;

    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicReference<Exception> exception = new AtomicReference<>();

    public MilvusSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            List<MilvusSinkState> milvusSinkStates) {
        this.batchWriter = new MilvusBufferBatchWriter(catalogTable, config);
        this.config = config;
        int batchInterval = config.get(MilvusSinkOptions.BATCH_INTERVAL);
        this.scheduler = batchInterval > 0 ? Executors.newSingleThreadScheduledExecutor() : null;
        if (scheduler != null) {
            log.info("create Milvus sink writer with batch interval: {}", batchInterval);
            scheduler.scheduleAtFixedRate(
                    new BatchWriterFlushRunnable(batchWriter),
                    0,
                    batchInterval,
                    TimeUnit.MILLISECONDS);
        }
        log.info("create Milvus sink writer success");
        log.info("MilvusSinkWriter config: " + config);
    }

    /**
     * write data to third party data receiver.
     *
     * @param element the data need be written.
     */
    @Override
    public void write(SeaTunnelRow element) {
        try {
            lock.lock();
            checkExceptionAndRethrow();
            batchWriter.addToBatch(element);
            if (batchWriter.needFlush()) {
                try {
                    // Flush the batch writer
                    batchWriter.flush();
                } catch (Exception e) {
                    log.error("flush Milvus sink writer failed", e);
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.WRITE_DATA_FAIL, e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * prepare the commit, will be called before {@link #snapshotState(long checkpointId)}. If you
     * need to use 2pc, you can return the commit info in this method, and receive the commit info
     * in {@link SinkCommitter#commit(List)}. If this method failed (by throw exception), **Only**
     * Spark engine will call {@link #abortPrepare()}
     *
     * @return the commit info need to commit
     */
    @Override
    public Optional<MilvusCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    /**
     * Used to abort the {@link #prepareCommit()}, if the prepareCommit failed, there is no
     * CommitInfoT, so the rollback work cannot be done by {@link SinkCommitter}. But we can use
     * this method to rollback side effects of {@link #prepareCommit()}. Only use it in Spark engine
     * at now.
     */
    @Override
    public void abortPrepare() {}

    /**
     * call it when SinkWriter close
     *
     * @throws IOException if close failed
     */
    @Override
    public void close() throws IOException {
        try {
            lock.lock();
            // make sure any errors from batch writer flush are handled
            checkExceptionAndRethrow();
            log.info("Stopping Milvus Client");
            batchWriter.flush();
            batchWriter.close();
            scheduler.shutdown();
            log.info("Stop Milvus Client success");
        } catch (Exception e) {
            log.error("Stop Milvus Client failed", e);
            throw new MilvusConnectorException(MilvusConnectionErrorCode.CLOSE_CLIENT_ERROR, e);
        } finally {
            lock.unlock();
        }
    }

    /** BatchWriterFlushRunnable is a runnable that will invoke batch writer flush method */
    private class BatchWriterFlushRunnable implements Runnable {
        private final MilvusBufferBatchWriter batchWriter;

        public BatchWriterFlushRunnable(MilvusBufferBatchWriter batchWriter) {
            this.batchWriter = batchWriter;
        }

        @Override
        public void run() {
            try {
                lock.lock();
                if (batchWriter.cachedSize() == 0) {
                    return;
                }
                batchWriter.flush();
            } catch (Exception e) {
                log.error("flush Milvus sink writer failed", e);
                exception.compareAndSet(null, e);
            } finally {
                lock.unlock();
            }
        }
    }

    /** Check exception in current thread, if exception is not null, throw it. */
    private void checkExceptionAndRethrow() {
        if (exception.get() != null) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL, exception.get());
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.imap.storage.file.common.WALLSMWriter;
import org.apache.seatunnel.engine.imap.storage.file.common.WALWriter;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.asLong;
import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.checkLongPositive;

@Slf4j
public class WALCompactionDisruptor extends AbstractWALDisruptor {
    private final ScheduledExecutorService compactionScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "wal-compaction");
                        t.setDaemon(true);
                        return t;
                    });

    private long compactionInterval = 60L * 1000;

    public WALCompactionDisruptor(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            String parentPath,
            Serializer serializer,
            Map<String, Object> config) {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor =
                new Disruptor<>(
                        FileWALEvent.FACTORY,
                        DEFAULT_RING_BUFFER_SIZE,
                        threadFactory,
                        ProducerType.SINGLE,
                        new BlockingWaitStrategy());

        WALWriter writer;
        try {
            writer =
                    new WALLSMWriter(
                            fs, fileConfiguration, new Path(parentPath), serializer, config);
        } catch (IOException e) {
            throw new IMapStorageException(
                    e, "create new current writer failed, parent path is %s", parentPath);
        }

        disruptor.handleEventsWithWorkerPool(new WALWorkHandler(writer));

        disruptor.start();

        long interval =
                asLong(
                        config.get(FileConstants.FileInitProperties.COMPACTION_INTERVAL),
                        this.compactionInterval);
        checkLongPositive(FileConstants.FileInitProperties.COMPACTION_INTERVAL, interval);
        this.compactionInterval = interval;

        compactionScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        writer.compaction();
                    } catch (Exception e) {
                        log.error("Compaction failed", e);
                    }
                },
                compactionInterval,
                compactionInterval,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        // we can wait for 10 seconds, so that backlog can be committed
        try {
            tryPublish(null, WALEventType.CLOSED, 0L);
            isClosed = true;

            disruptor.shutdown(DEFAULT_CLOSE_WAIT_TIME_SECONDS, TimeUnit.SECONDS);

            compactionScheduler.shutdown();
            try {
                if (!compactionScheduler.awaitTermination(
                        DEFAULT_CLOSE_WAIT_TIME_SECONDS, TimeUnit.SECONDS)) {
                    log.warn(
                            "Compaction scheduler did not terminate in 5 seconds, forcing shutdown");
                    compactionScheduler.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("Compaction scheduler termination interrupted, forcing shutdown", ie);
                compactionScheduler.shutdownNow();
            }

        } catch (TimeoutException e) {
            log.error("WALCompactionDisruptor close timeout error", e);
            throw new IMapStorageException("WALCompactionDisruptor close timeout error", e);
        }
    }
}

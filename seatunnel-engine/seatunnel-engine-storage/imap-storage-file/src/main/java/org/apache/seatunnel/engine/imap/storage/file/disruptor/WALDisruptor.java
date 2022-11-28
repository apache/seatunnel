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

import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WALDisruptor implements Closeable {

    private volatile Disruptor<FileWALEvent> disruptor;

    private static final int DEFAULT_RING_BUFFER_SIZE = 256 * 1024;

    private static final int DEFAULT_CLOSE_WAIT_TIME_SECONDS = 5;

    private boolean isClosed = false;

    private static final EventTranslatorThreeArg<FileWALEvent, IMapFileData, WALEventType, Long> TRANSLATOR =
        (event, sequence, data, walEventStatus, requestId) -> {
            event.setData(data);
            event.setType(walEventStatus);
            event.setRequestId(requestId);
        };

    public WALDisruptor(FileSystem fs, String parentPath, Serializer serializer) {
        //todo should support multi thread producer
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor = new Disruptor<>(FileWALEvent.FACTORY,
            DEFAULT_RING_BUFFER_SIZE, threadFactory,
            ProducerType.SINGLE,
            new BlockingWaitStrategy());

        disruptor.handleEventsWithWorkerPool(new WALWorkHandler(fs, parentPath, serializer));

        disruptor.start();
    }

    public boolean tryPublish(IMapFileData message, WALEventType status, Long requestId) {
        if (isClosed()) {
            return false;
        }
        disruptor.getRingBuffer().publishEvent(TRANSLATOR, message, status, requestId);
        return true;
    }

    public boolean tryAppendPublish(IMapFileData message, long requestId) {
        return this.tryPublish(message, WALEventType.APPEND, requestId);
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        //we can wait for 5 seconds, so that backlog can be committed
        try {
            tryPublish(null, WALEventType.CLOSED, 0L);
            isClosed = true;
            disruptor.shutdown(DEFAULT_CLOSE_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("WALDisruptor close timeout error", e);
            throw new IMapStorageException("WALDisruptor close timeout error", e);
        }
    }
}

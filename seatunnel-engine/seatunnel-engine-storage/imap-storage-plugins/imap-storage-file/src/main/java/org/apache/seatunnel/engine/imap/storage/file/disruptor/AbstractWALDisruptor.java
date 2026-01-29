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

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.dsl.Disruptor;

import java.io.Closeable;

public abstract class AbstractWALDisruptor implements Closeable {
    protected Disruptor<FileWALEvent> disruptor;

    protected static final int DEFAULT_RING_BUFFER_SIZE = 1024;

    protected static final int DEFAULT_CLOSE_WAIT_TIME_SECONDS = 5;

    protected volatile boolean isClosed = false;

    protected static final EventTranslatorThreeArg<FileWALEvent, IMapFileData, WALEventType, Long>
            TRANSLATOR =
                    (event, sequence, data, walEventStatus, requestId) -> {
                        event.setData(data);
                        event.setType(walEventStatus);
                        event.setRequestId(requestId);
                    };

    public boolean isClosed() {
        return isClosed;
    }

    public boolean tryPublish(IMapFileData message, WALEventType status, long requestId) {
        if (isClosed()) {
            return false;
        }
        disruptor.getRingBuffer().publishEvent(TRANSLATOR, message, status, requestId);
        return true;
    }

    public boolean tryAppendPublish(IMapFileData message, long requestId) {
        return this.tryPublish(message, WALEventType.APPEND, requestId);
    }
}

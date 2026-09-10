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
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALWriter;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFuture;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.lmax.disruptor.WorkHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Single-threaded Disruptor consumer that appends WAL frames.
 *
 * <p>After any APPEND write failure the handler fail-closes further APPEND attempts: continuing to
 * write on the same open stream could place a complete frame after a partially written one, and
 * {@code DefaultReader} cannot resync past a mid-file torn frame (it stops when a length prefix
 * claims more bytes than remain). Leaving any partial frame as a trailing incomplete record keeps
 * prior complete records recoverable; see {@code DefaultReaderTornTrailingRecordTest} and {@code
 * DefaultReaderTornMidFileRecordTest}. Blind {@code fs.create} reopen is intentionally avoided
 * because it would truncate the fixed {@code wal.txt} path.
 */
@Slf4j
public class WALWorkHandler implements WorkHandler<FileWALEvent> {

    private WALWriter writer;

    /**
     * When true, further APPEND events fail without touching the stream so a possible torn trailer
     * cannot become a mid-file tear.
     */
    private boolean appendBlockedAfterWriteFailure;

    public WALWorkHandler(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            String parentPath,
            Serializer serializer) {
        try {
            writer = new WALWriter(fs, fileConfiguration, new Path(parentPath), serializer);
        } catch (IOException e) {
            throw new IMapStorageException(
                    e, "create new current writer failed, parent path is %s", parentPath);
        }
    }

    @Override
    public void onEvent(FileWALEvent fileWALEvent) throws Exception {
        log.debug("write data to orc file");
        walEvent(fileWALEvent.getData(), fileWALEvent.getType(), fileWALEvent.getRequestId());
    }

    private void walEvent(IMapFileData iMapFileData, WALEventType type, long requestId)
            throws Exception {
        if (type == WALEventType.APPEND) {
            boolean writeSuccess = true;
            // Fail-closed after a prior write failure: do not append more bytes on a stream that
            // may already end in a torn frame (DefaultReader cannot resync mid-file).
            if (appendBlockedAfterWriteFailure) {
                log.warn(
                        "WAL APPEND blocked after a previous write failure, requestId is {}",
                        requestId);
                executeResponse(requestId, false);
                return;
            }
            // Catch all failures so RequestFuture.done() is always published. Narrowing this to
            // IOException previously allowed RuntimeException to kill the single WAL worker and
            // leave callers blocked until their wait timeout.
            try {
                writer.write(iMapFileData);
            } catch (Exception e) {
                writeSuccess = false;
                appendBlockedAfterWriteFailure = true;
                log.error("write orc file error, walEventBean is {} ", iMapFileData, e);
            }
            // Never let response publishing kill the sole disruptor consumer.
            executeResponse(requestId, writeSuccess);
            return;
        }

        if (type == WALEventType.CLOSED) {
            // close writer and archive. Intentionally unguarded: CLOSED is published once during
            // WALDisruptor/storage shutdown, so a failure here does not wedge steady-state APPEND
            // persistence the way an escaping write exception would.
            writer.close();
        }
    }

    private void executeResponse(long requestId, boolean success) {
        try {
            RequestFuture future = RequestFutureCache.get(requestId);
            if (future == null) {
                log.warn("requestId is {} not found in RequestFutureCache", requestId);
                return;
            }
            future.done(success);
        } catch (Exception e) {
            log.error("response error, requestId is {} ", requestId, e);
        }
    }
}

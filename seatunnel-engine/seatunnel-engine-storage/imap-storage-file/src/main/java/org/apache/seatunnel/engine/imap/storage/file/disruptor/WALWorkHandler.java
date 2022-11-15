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
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;
import org.apache.seatunnel.engine.imap.storage.file.orc.OrcWriter;

import com.lmax.disruptor.WorkHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * NOTICE:
 * Single thread to write data to orc file.
 */
@Slf4j
public class WALWorkHandler implements WorkHandler<FileWALEvent> {

    /**
     * we used the "current-${System.System.nanoTime()}" for the current orc file name
     */
    private static final String CURRENT_FILE_NAME_PREFIX = "/current/";

    private OrcWriter currentWriter;

    private Configuration configuration;

    private String parentPath;

    private long lastDataWriterTime = System.currentTimeMillis();

    private static final long MINIMUM_INTERVAL_SINCE_LAST_WRITE_SECONDS_TIME = 1000 * 60;

    public WALWorkHandler(Configuration configuration, String parentPath) {
        this.configuration = configuration;
        this.parentPath = parentPath;
        createNewCurrentWriter();
    }

    private void createNewCurrentWriter() {
        String currentFileName = parentPath + CURRENT_FILE_NAME_PREFIX + System.nanoTime();
        try {
            currentWriter = new OrcWriter(new Path(currentFileName), configuration);
        } catch (IOException e) {
            throw new IMapStorageException(e, "create new current writer failed, parent path is s%", parentPath);
        }
    }

    /**
     * closed the current writer
     * and create a new writer and set it to current writer
     */
    private void archiveAndCreateNewFile() {
        // should config
        if (System.currentTimeMillis() - lastDataWriterTime < MINIMUM_INTERVAL_SINCE_LAST_WRITE_SECONDS_TIME) {
            return;
        }
        try {
            currentWriter.archive(parentPath);
        } catch (IOException e) {
            log.error("archive file failed, current file parent path is {} ", parentPath, e);
        }
        createNewCurrentWriter();
    }

    @Override
    public void onEvent(FileWALEvent fileWALEvent) throws Exception {
        log.debug("write data to orc file");
        walEvent(fileWALEvent.getData(), fileWALEvent.getType(), fileWALEvent.getRequestId());
    }

    private void walEvent(IMapFileData iMapFileData, WALEventType type, long requestId) throws IOException {
        if (type == WALEventType.APPEND) {
            boolean writeSuccess = true;
            // write to current writer
            try {
                currentWriter.write(iMapFileData);
            } catch (IOException e) {
                writeSuccess = false;
                log.error("write orc file error, walEventBean is {} ", iMapFileData, e);
            }
            lastDataWriterTime = System.currentTimeMillis();
            // return the result to the client
            executeResponse(requestId, writeSuccess);
            return;
        }

        if (type == WALEventType.SCHEDULER_ARCHIVE) {
            // archive current writer
            archiveAndCreateNewFile();
            return;
        }
        if (type == WALEventType.IMMEDIATE_ARCHIVE) {
            //close writer and archive
            currentWriter.archive(parentPath);
            executeResponse(requestId, true);
            createNewCurrentWriter();
            return;
        }
        if (type == WALEventType.CLOSED) {
            //close writer and archive
            currentWriter.archive(parentPath);
        }
    }

    private void executeResponse(long requestId, boolean success) {
        try {
            RequestFutureCache.get(requestId).done(success);
        } catch (RuntimeException e) {
            log.error("response error, requestId is {} ", requestId, e);
        }
    }

}

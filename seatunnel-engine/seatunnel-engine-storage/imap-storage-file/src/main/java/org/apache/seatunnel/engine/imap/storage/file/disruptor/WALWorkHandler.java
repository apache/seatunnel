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
import org.apache.seatunnel.engine.imap.storage.file.common.WALWriter;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;

import com.lmax.disruptor.WorkHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * NOTICE:
 * Single thread to write data to orc file.
 */
@Slf4j
public class WALWorkHandler implements WorkHandler<FileWALEvent> {

    private WALWriter writer;

    public WALWorkHandler(FileSystem fs, String parentPath, Serializer serializer) {
        try {
            writer = new WALWriter(fs, new Path(parentPath), serializer);
        } catch (IOException e) {
            throw new IMapStorageException(e, "create new current writer failed, parent path is %s", parentPath);
        }
    }

    @Override
    public void onEvent(FileWALEvent fileWALEvent) throws Exception {
        log.debug("write data to orc file");
        walEvent(fileWALEvent.getData(), fileWALEvent.getType(), fileWALEvent.getRequestId());
    }

    private void walEvent(IMapFileData iMapFileData, WALEventType type, long requestId) throws Exception {
        if (type == WALEventType.APPEND) {
            boolean writeSuccess = true;
            // write to current writer
            try {
                writer.write(iMapFileData);
            } catch (IOException e) {
                writeSuccess = false;
                log.error("write orc file error, walEventBean is {} ", iMapFileData, e);
            }
            // return the result to the client
            executeResponse(requestId, writeSuccess);
            return;
        }

        if (type == WALEventType.CLOSED) {
            //close writer and archive
            writer.close();
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

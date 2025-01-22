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

package org.apache.seatunnel.connectors.tailfile.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.tailfile.source.tailfile.FileManager;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class TailFileSourceReader implements SourceReader<SeaTunnelRow, TailFileSourceSplit> {
    private final Context context;
    private final FileManager fileManager;
    private volatile boolean noMoreSplit;

    public TailFileSourceReader(Context context, TailFileSourceConfig config) {
        this.context = context;
        this.fileManager = new FileManager(config);
    }

    @Override
    public void open() {
        log.info("Open source reader {}", context.getIndexOfSubtask());
    }

    @Override
    public void close() {
        log.info("Close source reader {}", context.getIndexOfSubtask());
        fileManager.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        List<Long> fileInodes = Collections.emptyList();
        synchronized (output.getCheckpointLock()) {
            if (!fileManager.isEmpty()) {
                fileInodes = fileManager.checkNeedTailFiles();
            }
        }

        for (Long fileInode : fileInodes) {
            synchronized (output.getCheckpointLock()) {
                try {
                    fileManager.tailFile(fileInode, output::collect);
                } catch (Exception e) {
                    log.error("Field to tail file {}", fileInode, e);
                }
            }
        }
        if (!fileInodes.isEmpty()) {
            return;
        }

        if (noMoreSplit && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            context.signalNoMoreElement();
        } else if (fileManager.isEmpty()) {
            context.sendSplitRequest();
            Thread.sleep(1000L);
        }
    }

    @Override
    public List<TailFileSourceSplit> snapshotState(long checkpointId) {
        return fileManager.snapshot();
    }

    @Override
    public void addSplits(List<TailFileSourceSplit> splits) {
        fileManager.register(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}

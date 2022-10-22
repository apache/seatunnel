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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class JdbcSourceReader implements SourceReader<SeaTunnelRow, JdbcSourceSplit> {

    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    SourceReader.Context context;
    Deque<JdbcSourceSplit> splits = new LinkedList<>();
    JdbcInputFormat inputFormat;
    boolean noMoreSplit;

    public JdbcSourceReader(JdbcInputFormat inputFormat, SourceReader.Context context) {
        this.inputFormat = inputFormat;
        this.context = context;
    }

    @Override
    public void open() throws Exception {
        inputFormat.openInputFormat();
    }

    @Override
    public void close() throws IOException {
        inputFormat.closeInputFormat();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            JdbcSourceSplit split = splits.poll();
            if (null != split) {
                inputFormat.open(split);
                while (!inputFormat.reachedEnd()) {
                    SeaTunnelRow seaTunnelRow = inputFormat.nextRecord();
                    output.collect(seaTunnelRow);
                }
                inputFormat.close();
            } else if (noMoreSplit) {
                // signal to the source that we have reached the end of the data.
                LOG.info("Closed the bounded jdbc source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<JdbcSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}

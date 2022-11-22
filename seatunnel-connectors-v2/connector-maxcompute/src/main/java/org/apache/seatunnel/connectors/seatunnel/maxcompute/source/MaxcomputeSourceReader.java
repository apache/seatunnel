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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.maxcomputeclient.MaxcomputeInputFormat;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class MaxcomputeSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final SingleSplitReaderContext context;

    private final MaxcomputeInputFormat maxcomputeInputFormat;

    boolean noMoreSplit;

    public MaxcomputeSourceReader(MaxcomputeInputFormat kuduInputFormat, SingleSplitReaderContext context) {
        this.context = context;
        this.maxcomputeInputFormat = kuduInputFormat;
    }

    @Override
    public void open() {
        maxcomputeInputFormat.openInputFormat();
    }

    @Override
    public void close() {
        maxcomputeInputFormat.closeInputFormat();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        RecordReader recordReader = maxcomputeInputFormat.getTunnelReader();
        if (null != recordReader) {
            Record record;
            while ((record = recordReader.read()) != null) {
                SeaTunnelRow seaTunnelRow = MaxcomputeInputFormat.getSeaTunnelRowData(record, maxcomputeInputFormat.getSeaTunnelRowType());
                output.collect(seaTunnelRow);
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }
}

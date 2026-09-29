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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import java.util.Map;
import java.util.function.Supplier;

public class FlussSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                FlussRecord, SeaTunnelRow, FlussSourceSplit, FlussSourceSplitState> {

    public FlussSourceReader(
            Supplier<SplitReader<FlussRecord, FlussSourceSplit>> splitReaderSupplier,
            RecordEmitter<FlussRecord, SeaTunnelRow, FlussSourceSplitState> recordEmitter,
            SourceReaderOptions options,
            SourceReader.Context context) {
        super(splitReaderSupplier, recordEmitter, options, context);
    }

    @Override
    protected void onSplitFinished(Map<String, FlussSourceSplitState> finishedSplitIds) {
        // no external offset commit required; positions are held in the checkpoint state
    }

    @Override
    protected FlussSourceSplitState initializedState(FlussSourceSplit split) {
        return new FlussSourceSplitState(split);
    }

    @Override
    protected FlussSourceSplit toSplitType(String splitId, FlussSourceSplitState splitState) {
        return splitState.toFlussSourceSplit();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no external offset commit required; positions are held in the checkpoint state
    }
}

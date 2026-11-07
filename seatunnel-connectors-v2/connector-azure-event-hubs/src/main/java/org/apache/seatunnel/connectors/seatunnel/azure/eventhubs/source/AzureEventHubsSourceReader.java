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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import java.util.Map;
import java.util.function.Supplier;

public class AzureEventHubsSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                EventHubsRecord,
                SeaTunnelRow,
                AzureEventHubsSourceSplit,
                AzureEventHubsSourceSplitState> {

    public AzureEventHubsSourceReader(
            Supplier<SplitReader<EventHubsRecord, AzureEventHubsSourceSplit>> splitReaderSupplier,
            RecordEmitter<EventHubsRecord, SeaTunnelRow, AzureEventHubsSourceSplitState>
                    recordEmitter,
            SourceReaderOptions options,
            SourceReader.Context context) {
        super(splitReaderSupplier, recordEmitter, options, context);
    }

    @Override
    protected void onSplitFinished(Map<String, AzureEventHubsSourceSplitState> finishedSplitIds) {
        // Event Hubs partitions are unbounded and are never completed by this source.
    }

    @Override
    protected AzureEventHubsSourceSplitState initializedState(AzureEventHubsSourceSplit split) {
        return new AzureEventHubsSourceSplitState(split);
    }

    @Override
    protected AzureEventHubsSourceSplit toSplitType(
            String splitId, AzureEventHubsSourceSplitState splitState) {
        return splitState.toSourceSplit();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Event Hubs has no connector-side offset commit; SeaTunnel checkpoints own the position.
    }
}

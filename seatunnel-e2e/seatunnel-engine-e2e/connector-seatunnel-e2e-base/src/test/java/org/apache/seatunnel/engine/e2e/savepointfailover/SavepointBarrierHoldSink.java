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

package org.apache.seatunnel.engine.e2e.savepointfailover;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

/**
 * Test-only sink without committers or writer state that lets {@link
 * SavepointBarrierHoldSinkWriter#snapshotState(long)} block on {@link SavepointBarrierHoldGate}
 * until the test releases it.
 *
 * <p>Having no committer keeps the pipeline down to a split enumerator plus one chained
 * source-and-sink task per parallelism, so the barrier acknowledgement of every task goes straight
 * through the held writer.
 */
public class SavepointBarrierHoldSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    /** Key that binds this sink to the gate armed by the test. */
    private final String holdKey;

    public SavepointBarrierHoldSink(String holdKey) {
        this.holdKey = holdKey;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new SavepointBarrierHoldSinkWriter(context, holdKey);
    }

    @Override
    public String getPluginName() {
        return SavepointBarrierHoldSinkFactory.IDENTIFIER;
    }
}

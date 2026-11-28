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

package org.apache.seatunnel.engine.e2e.checkpointtimeout;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Test-only sink that parks one checkpoint snapshot on {@link CheckpointTimeoutHoldGate} and
 * checkpoints a state string that identifies the checkpoint it belongs to.
 *
 * <p>The state is what makes the recovery side verifiable: a pipeline restored after the held
 * checkpoint expired must hand the restored writer the state of the last <em>completed</em>
 * checkpoint, never the state of the aborted one. The sink does not implement any protocol or
 * commit logic of its own, so a failure of the test can only be an engine scheduling, recovery or
 * checkpoint-coordination problem.
 */
public class CheckpointTimeoutHoldSink extends AbstractSimpleSink<SeaTunnelRow, String> {

    /** Prefix of the single state entry written by each checkpoint. */
    public static final String STATE_PREFIX = "checkpoint:";

    private final String holdKey;

    private final int blockOnSnapshot;

    public CheckpointTimeoutHoldSink(String holdKey, int blockOnSnapshot) {
        this.holdKey = holdKey;
        this.blockOnSnapshot = blockOnSnapshot;
    }

    @Override
    public CheckpointTimeoutHoldSinkWriter createWriter(SinkWriter.Context context) {
        return new CheckpointTimeoutHoldSinkWriter(holdKey, blockOnSnapshot);
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, String> restoreWriter(
            SinkWriter.Context context, List<String> states) throws IOException {
        CheckpointTimeoutHoldGate.recordRestoredState(
                holdKey, states == null || states.isEmpty() ? null : states.get(0));
        return createWriter(context);
    }

    @Override
    public Optional<Serializer<String>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return CheckpointTimeoutHoldSinkFactory.IDENTIFIER;
    }
}

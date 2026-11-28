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

package org.apache.seatunnel.engine.e2e.savepointcancel;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Test-only sink that keeps one checkpoint snapshot unacknowledged through {@link
 * SavepointCancelGate}.
 *
 * <p>The sink implements no protocol of its own: it consumes rows and returns a trivial checkpoint
 * state. Every engine-visible effect therefore comes from the delayed barrier acknowledgement, not
 * from connector or format semantics.
 */
public class SavepointCancelHoldSink extends AbstractSimpleSink<SeaTunnelRow, String> {

    /** Prefix of the single state entry written by each checkpoint. */
    public static final String STATE_PREFIX = "savepoint-cancel:";

    private final String holdKey;

    private final int blockOnSnapshot;

    public SavepointCancelHoldSink(String holdKey, int blockOnSnapshot) {
        this.holdKey = holdKey;
        this.blockOnSnapshot = blockOnSnapshot;
    }

    @Override
    public SavepointCancelHoldSinkWriter createWriter(SinkWriter.Context context) {
        return new SavepointCancelHoldSinkWriter(holdKey, blockOnSnapshot);
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, String> restoreWriter(
            SinkWriter.Context context, List<String> states) throws IOException {
        // A restored writer never holds: the snapshot under test belongs to one job attempt only.
        return createWriter(context);
    }

    @Override
    public Optional<Serializer<String>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return SavepointCancelHoldSinkFactory.IDENTIFIER;
    }
}

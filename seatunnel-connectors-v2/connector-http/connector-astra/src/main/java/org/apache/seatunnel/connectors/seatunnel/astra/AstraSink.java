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

package org.apache.seatunnel.connectors.seatunnel.astra;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Creates one ordered Astra writer for one declared sink and restores previously acknowledged
 * progress. Writer state binds checkpoint replay to the same destination and projection.
 */
public final class AstraSink implements SeaTunnelSink<SeaTunnelRow, AstraSink.State, Void, Void> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Astra";
    private final AstraConfig config;
    private final CatalogTable table;
    private String job;

    AstraSink(AstraConfig config, CatalogTable table) {
        this.config = config;
        this.table = table;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(table);
    }

    @Override
    public void setJobContext(JobContext context) {
        job = context.getJobId();
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, State> createWriter(SinkWriter.Context context)
            throws IOException {
        String identity = job == null ? UUID.randomUUID().toString() : job;
        return create(context, new State(config.fingerprint, identity, 0, false));
    }

    /**
     * Restores the completed mutation sequence and avoids re-running an acknowledged scope cleanup.
     * Authentication can rotate without changing the logical checkpoint identity.
     */
    @Override
    public SinkWriter<SeaTunnelRow, Void, State> restoreWriter(
            SinkWriter.Context context, List<State> states) throws IOException {
        if (states.isEmpty()) {
            return createWriter(context);
        }
        State state = states.get(0);
        if (states.size() != 1
                || !config.fingerprint.equals(state.fingerprint)
                || state.position < 0
                || state.run == null
                || state.run.isEmpty()) {
            throw new IOException("Astra sink: writer checkpoint does not match the sink");
        }
        return create(context, state);
    }

    @Override
    public Optional<Serializer<State>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    private AstraWriter create(SinkWriter.Context context, State state) throws IOException {
        if (context.getNumberOfParallelSubtasks() != 1 || context.getIndexOfSubtask() != 0) {
            throw new IOException("Astra sink: sink parallelism must be 1");
        }
        AstraWriter writer = new AstraWriter(config, table.getSeaTunnelRowType(), state);
        context.enableDeferredTerminalWriteOutcomes();
        context.registerFlushAction(writer::flush);
        return writer;
    }

    /**
     * Stores the run and next sequence after a completed checkpoint plus the overwrite fence. The
     * serialized state contains no authentication or user data.
     */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final String run;
        final long position;
        final boolean overwriteDone;

        State(String fingerprint, String run, long position, boolean overwriteDone) {
            this.fingerprint = fingerprint;
            this.run = run;
            this.position = position;
            this.overwriteDone = overwriteDone;
        }
    }
}

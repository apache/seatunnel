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

package org.apache.seatunnel.connectors.seatunnel.aha;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises two source interface states that a running container cannot reliably pause on. AhaIT
 * drives actual checkpoints and savepoint recovery through Zeta and HTTPS.
 */
class AhaCheckpointBoundaryIT {
    @Test
    void restoresAnEnumeratorCheckpointFromBeforeSplitAssignment() throws Exception {
        AhaConfig config = configuration();
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<AhaProgress> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.emptySet());
        AhaEnumerator.State checkpoint;
        try (AhaEnumerator original = new AhaEnumerator(context, config, null)) {
            original.open();
            original.run();
            assertEquals(1, original.currentUnassignedSplitSize());
            checkpoint = copy(original.snapshotState(1));
        }
        Set<Integer> registered = Collections.singleton(0);
        try (AhaEnumerator restored = new AhaEnumerator(context, config, checkpoint)) {
            restored.open();
            assertEquals(1, restored.currentUnassignedSplitSize());
            when(context.registeredReaders()).thenReturn(registered);
            restored.registerReader(0);
            restored.run();
            restored.handleSplitRequest(0);
            assertEquals(0, restored.currentUnassignedSplitSize());
            org.mockito.Mockito.verify(context)
                    .assignSplit(
                            org.mockito.ArgumentMatchers.eq(0),
                            org.mockito.ArgumentMatchers.any(AhaProgress.class));
            org.mockito.Mockito.verify(context).signalNoMoreSplits(0);
        }
    }

    @Test
    void restoresTerminalReaderStateAndSignalsEndOnlyOnce() throws Exception {
        AhaConfig config = configuration();
        AhaProgress terminal = new AhaProgress(config.fingerprint);
        terminal.nextResource(1);
        SourceReader.Context reader = mock(SourceReader.Context.class);
        when(reader.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    private final Object checkpointLock = new Object();

                    @Override
                    public void collect(SeaTunnelRow record) {
                        fail("the restored terminal source cannot emit records");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return checkpointLock;
                    }
                };
        try (AhaReader restored = new AhaReader(config, reader, "Aha")) {
            restored.open();
            assertTrue(restored.snapshotState(1).isEmpty());
            restored.addSplits(Collections.singletonList(copy(terminal)));
            restored.pollNext(collector);
            restored.pollNext(collector);
            List<AhaProgress> state = restored.snapshotState(2);
            assertEquals(1, state.size());
            assertTrue(state.get(0).finished);
            org.mockito.Mockito.verify(reader).signalNoMoreElement();
        }
    }

    private static AhaConfig configuration() {
        Map<String, Object> options = new HashMap<>();
        options.put("api_url", "https://fixture.aha.io");
        options.put("token", "fixture-only-aha-token");
        options.put("resources", Collections.singletonList("goals"));
        return new AhaConfig(ReadonlyConfig.fromMap(options));
    }

    @SuppressWarnings("unchecked")
    private static <T> T copy(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) input.readObject();
        }
    }
}

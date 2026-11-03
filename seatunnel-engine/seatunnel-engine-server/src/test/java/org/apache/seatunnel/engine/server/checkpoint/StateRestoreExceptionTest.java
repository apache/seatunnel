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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.SerializationException;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StateRestoreExceptionTest {

    private static final TaskLocation LOCATION =
            new TaskLocation(new TaskGroupLocation(42L, 3, 1L), 1L, 0);

    /** State class of the "new" connector version; the checkpoint was written with UID 1. */
    public static class UpgradedState implements Serializable {
        private static final long serialVersionUID = 2L;

        private final String value;

        public UpgradedState(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof UpgradedState && ((UpgradedState) o).value.equals(value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    /**
     * Returns state bytes as an older connector version wrote them: an {@link UpgradedState} whose
     * class had serialVersionUID 1. Reading them fails with the {@link InvalidClassException} an
     * upgrade produces.
     */
    public static byte[] stateWrittenByAnOlderVersion() throws IOException {
        byte[] bytes = new DefaultSerializer<UpgradedState>().serialize(new UpgradedState("old"));
        byte[] name = UpgradedState.class.getName().getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i + 3 + name.length + 8 <= bytes.length; i++) {
            if (bytes[i] == 0x72 && matchesName(bytes, i + 1, name)) {
                ByteBuffer.wrap(bytes, i + 3 + name.length, 8).putLong(1L);
                return bytes;
            }
        }
        throw new IllegalStateException("class descriptor not found in the serialized state");
    }

    private static boolean matchesName(byte[] bytes, int offset, byte[] name) {
        if (((bytes[offset] & 0xff) << 8 | (bytes[offset + 1] & 0xff)) != name.length) {
            return false;
        }
        for (int i = 0; i < name.length; i++) {
            if (bytes[offset + 2 + i] != name[i]) {
                return false;
            }
        }
        return true;
    }

    /** Asserts the message format and that the original exception is kept as the cause. */
    public static void assertUpgradeFailure(
            StateRestoreException e, String actionName, String pluginName, long job, int pipeline) {
        Assertions.assertTrue(
                e.getMessage()
                        .startsWith(
                                "Cannot restore state of action '"
                                        + actionName
                                        + "' (job "
                                        + job
                                        + ", pipeline "
                                        + pipeline
                                        + "): state produced by version unknown is not readable"
                                        + " by this connector. See the upgrade notes for "
                                        + pluginName
                                        + ". Cause: "
                                        + SerializationException.class.getName()
                                        + ": "
                                        + InvalidClassException.class.getName()),
                e.getMessage());
        Assertions.assertTrue(e.getCause() instanceof SerializationException, e.toString());
        Assertions.assertTrue(
                e.getCause().getCause() instanceof InvalidClassException, e.toString());
    }

    @Test
    void readableStateIsReturnedUnchanged() throws IOException {
        DefaultSerializer<UpgradedState> serializer = new DefaultSerializer<>();
        byte[] bytes = serializer.serialize(new UpgradedState("current"));

        Assertions.assertEquals(
                new UpgradedState("current"),
                StateRestoreException.deserialize(
                        serializer, bytes, "Source[0]-Fake", "FakeSource", LOCATION));
    }

    @Test
    void stateOfAnOlderVersionNamesTheActionAndKeepsTheCause() throws IOException {
        byte[] bytes = stateWrittenByAnOlderVersion();

        StateRestoreException e =
                Assertions.assertThrows(
                        StateRestoreException.class,
                        () ->
                                StateRestoreException.deserialize(
                                        new DefaultSerializer<UpgradedState>(),
                                        bytes,
                                        "Source[0]-Fake",
                                        "FakeSource",
                                        LOCATION));

        assertUpgradeFailure(e, "Source[0]-Fake", "FakeSource", 42L, 3);
        Assertions.assertTrue(
                e.getMessage()
                        .contains(
                                "stream classdesc serialVersionUID = 1, local class"
                                        + " serialVersionUID = 2"),
                e.getMessage());
    }

    @Test
    void sinkPluginNameLooksThroughTheMultiTableWrapper() {
        SeaTunnelSink<?, ?, ?, ?> localFile = Mockito.mock(SeaTunnelSink.class);
        Mockito.when(localFile.getPluginName()).thenReturn("LocalFile");
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("db", "a"), localFile);
        sinks.put(TablePath.of("db", "b"), localFile);
        MultiTableSink wrapper = Mockito.mock(MultiTableSink.class);
        Mockito.when(wrapper.getPluginName()).thenReturn("MultiTableSink");
        Mockito.when(wrapper.getSinks()).thenReturn(sinks);

        Assertions.assertEquals("LocalFile", StateRestoreException.sinkPluginName(wrapper));
        Assertions.assertEquals("LocalFile", StateRestoreException.sinkPluginName(localFile));

        Mockito.when(wrapper.getSinks()).thenReturn(Collections.emptyMap());
        Assertions.assertEquals("MultiTableSink", StateRestoreException.sinkPluginName(wrapper));
    }
}

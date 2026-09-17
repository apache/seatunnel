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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Covers graceful member-removal marker writes around the Hazelcast shutdown lifecycle, including
 * the asynchronous clear on startup and its failure handling.
 */
class SeaTunnelServerShutdownTest {

    /**
     * Verifies that Hazelcast publishes the marker from {@code SHUTTING_DOWN}, before it marks the
     * node as shutting down and disables distributed-object proxies.
     */
    @Test
    void shouldMarkGracefulMemberRemovalOnGracefulShutdown() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        IMap<Address, Long> gracefulMemberRemovalIMap = mock(IMap.class);
        Address address = new Address("127.0.0.1", 5801);
        when(nodeEngine.getThisAddress()).thenReturn(address);
        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        when(hazelcastInstance.<Address, Long>getMap(Constant.IMAP_GRACEFUL_MEMBER_REMOVAL))
                .thenReturn(gracefulMemberRemovalIMap);

        createServer(nodeEngine).stateChanged(new LifecycleEvent(LifecycleState.SHUTTING_DOWN));

        verify(gracefulMemberRemovalIMap)
                .put(
                        eq(address),
                        anyLong(),
                        eq(Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS),
                        eq(TimeUnit.MILLISECONDS));
        verify(gracefulMemberRemovalIMap, never()).remove(address);
    }

    /**
     * Verifies that stale markers are cleared only after Hazelcast has joined the cluster and
     * published {@code STARTED}.
     */
    @Test
    void shouldClearGracefulMemberRemovalMarkerWhenHazelcastStarts() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        IMap<Address, Long> gracefulMemberRemovalIMap = mock(IMap.class);
        Address address = new Address("127.0.0.1", 5801);
        when(nodeEngine.getThisAddress()).thenReturn(address);
        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        when(hazelcastInstance.<Address, Long>getMap(Constant.IMAP_GRACEFUL_MEMBER_REMOVAL))
                .thenReturn(gracefulMemberRemovalIMap);
        createServer(nodeEngine).stateChanged(new LifecycleEvent(LifecycleState.STARTED));

        verify(gracefulMemberRemovalIMap).remove(address);
        verify(gracefulMemberRemovalIMap, never()).removeAsync(address);
    }

    /**
     * Ensures startup does not touch the distributed map before the member has joined the cluster.
     */
    @Test
    void shouldNotClearGracefulMemberRemovalMarkerBeforeHazelcastStarts() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);

        createServer(nodeEngine).stateChanged(new LifecycleEvent(LifecycleState.STARTING));

        verifyNoInteractions(nodeEngine);
    }

    /**
     * Managed service cleanup never attempts a late marker write after Hazelcast turns passive; the
     * lifecycle listener must have written it before node shutdown begins.
     */
    @Test
    void shouldNotMarkGracefulMemberRemovalDuringManagedServiceShutdown() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);

        createServer(nodeEngine).shutdown(false);

        verifyNoInteractions(nodeEngine);
    }

    /**
     * Covers the failure branch of the marker clear. A transient map failure must not escape the
     * Hazelcast lifecycle callback. The warning text is not asserted because the class logs through
     * a static Hazelcast logger.
     */
    @Test
    void shouldAbsorbFailedMarkerClearAfterStartup() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        IMap<Address, Long> gracefulMemberRemovalIMap = mock(IMap.class);
        Address address = new Address("127.0.0.1", 5801);
        when(nodeEngine.getThisAddress()).thenReturn(address);
        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        when(hazelcastInstance.<Address, Long>getMap(Constant.IMAP_GRACEFUL_MEMBER_REMOVAL))
                .thenReturn(gracefulMemberRemovalIMap);
        when(gracefulMemberRemovalIMap.remove(address))
                .thenThrow(new IllegalStateException("map service unavailable"));

        SeaTunnelServer seaTunnelServer = createServer(nodeEngine);

        Assertions.assertDoesNotThrow(
                () -> seaTunnelServer.stateChanged(new LifecycleEvent(LifecycleState.STARTED)));
        verify(gracefulMemberRemovalIMap).remove(address);
        verify(gracefulMemberRemovalIMap, never()).removeAsync(address);
    }

    private static SeaTunnelServer createServer(NodeEngineImpl nodeEngine) throws Exception {
        SeaTunnelServer seaTunnelServer = new SeaTunnelServer(mock(SeaTunnelConfig.class));
        setField(seaTunnelServer, "nodeEngine", nodeEngine);
        setField(seaTunnelServer, "engineContext", mock(SeaTunnelEngineContext.class));
        return seaTunnelServer;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}

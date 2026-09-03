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

import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Covers graceful member-removal marker writes around the Hazelcast shutdown lifecycle. */
class SeaTunnelServerShutdownTest {

    /** Verifies that Hazelcast invokes the marker write before managed-service teardown. */
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

        SeaTunnelServer seaTunnelServer = createServer(nodeEngine);

        seaTunnelServer.onShutdown(30, TimeUnit.SECONDS);

        verify(gracefulMemberRemovalIMap)
                .put(
                        eq(address),
                        anyLong(),
                        eq(Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS),
                        eq(TimeUnit.MILLISECONDS));
        verify(gracefulMemberRemovalIMap, never()).remove(address);
    }

    /**
     * Verifies that stale markers are cleared asynchronously while Hazelcast starts. The completion
     * stage lets startup continue before map services accept the remove operation.
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
        when(gracefulMemberRemovalIMap.removeAsync(address))
                .thenReturn(CompletableFuture.<Long>completedFuture(null));

        createServer(nodeEngine).stateChanged(new LifecycleEvent(LifecycleState.STARTING));

        verify(gracefulMemberRemovalIMap).removeAsync(address);
        verify(gracefulMemberRemovalIMap, never()).remove(address);
    }

    /** Ensures later lifecycle notifications cannot clear the marker during shutdown. */
    @Test
    void shouldNotClearGracefulMemberRemovalMarkerAfterHazelcastStarts() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);

        createServer(nodeEngine).stateChanged(new LifecycleEvent(LifecycleState.SHUTTING_DOWN));

        verifyNoInteractions(nodeEngine);
    }

    /** Managed service cleanup never attempts a late marker write after Hazelcast turns passive. */
    @Test
    void shouldNotMarkGracefulMemberRemovalDuringManagedServiceShutdown() throws Exception {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);

        createServer(nodeEngine).shutdown(false);

        verifyNoInteractions(nodeEngine);
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

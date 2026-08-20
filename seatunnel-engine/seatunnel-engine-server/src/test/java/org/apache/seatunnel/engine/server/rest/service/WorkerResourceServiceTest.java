/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceSnapshot;

import org.junit.jupiter.api.Test;

import com.hazelcast.spi.impl.NodeEngineImpl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkerResourceServiceTest {

    @Test
    void shouldReturnUnavailableSnapshotBeforeMasterElectionCompletes() {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        when(nodeEngine.getMasterAddress()).thenReturn(null);
        WorkerResourceService service = new TestWorkerResourceService(nodeEngine, null);

        WorkerResourceSnapshot snapshot = service.getWorkerResources();

        assertFalse(snapshot.isAvailable());
        assertTrue(snapshot.getCollectedAt() > 0);
        assertNotNull(snapshot.getWorkers());
        assertTrue(snapshot.getWorkers().isEmpty());
    }

    private static class TestWorkerResourceService extends WorkerResourceService {
        private final SeaTunnelServer seaTunnelServer;

        private TestWorkerResourceService(
                NodeEngineImpl nodeEngine, SeaTunnelServer seaTunnelServer) {
            super(nodeEngine);
            this.seaTunnelServer = seaTunnelServer;
        }

        @Override
        protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
            return seaTunnelServer;
        }
    }
}

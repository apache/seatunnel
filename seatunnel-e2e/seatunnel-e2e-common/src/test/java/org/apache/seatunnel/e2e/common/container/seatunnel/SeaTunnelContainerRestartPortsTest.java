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

package org.apache.seatunnel.e2e.common.container.seatunnel;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.Network;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SeaTunnelContainerRestartPortsTest {
    @Test
    void reusedWrapperClearsPreviousRestartPortsBeforeCreatingNewServer() throws Exception {
        SeaTunnelContainer container =
                Mockito.mock(SeaTunnelContainer.class, Mockito.CALLS_REAL_METHODS);
        Field cachedPorts = SeaTunnelContainer.class.getDeclaredField("restartedHostPorts");
        cachedPorts.setAccessible(true);
        cachedPorts.set(container, Collections.singletonMap(8080, 12345));
        IllegalStateException beforeDocker =
                new IllegalStateException("stop before Docker creation");
        Mockito.doThrow(beforeDocker).when(container).getDockerImage();

        assertSame(
                beforeDocker,
                assertThrows(
                        IllegalStateException.class,
                        () -> container.startUp(Mockito.mock(Network.class))));
        assertNull(cachedPorts.get(container));
    }
}

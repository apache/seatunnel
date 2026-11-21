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

package org.apache.seatunnel.resource.yarn.cli;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeatunnelYarnApplicationLifecycleTest {

    @Test
    void reportsNonzeroExitOnFatalApplicationError() {
        try (MockedStatic<YarnConfigurationUtils> configuration =
                mockStatic(YarnConfigurationUtils.class)) {
            configuration
                    .when(
                            () ->
                                    YarnConfigurationUtils.loadLocalized(
                                            YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME))
                    .thenThrow(new NoClassDefFoundError("missing Hadoop class"));
            assertEquals(1, SeatunnelYarnApplicationCli.runApplication());
        }
    }

    @Test
    void closesDriverAndPreservesMasterStartupError() throws Exception {
        SeaTunnelConfig config = new SeaTunnelConfig();
        ResourceManagerDriver<?> driver = mock(ResourceManagerDriver.class);
        LinkageError startupFailure = new LinkageError("startup");
        IllegalStateException closeFailure = new IllegalStateException("close");
        doThrow(closeFailure).when(driver).close();
        try (MockedStatic<SeaTunnelServerStarter> starter =
                mockStatic(SeaTunnelServerStarter.class)) {
            starter.when(
                            () ->
                                    SeaTunnelServerStarter.createHazelcastInstance(
                                            eq(config),
                                            isNull(),
                                            any(ResourceManagerFactory.class)))
                    .thenThrow(startupFailure);
            assertSame(
                    startupFailure,
                    assertThrows(
                            LinkageError.class,
                            () ->
                                    SeatunnelYarnApplicationCli.startMaster(
                                            config,
                                            mock(ApplicationSpecification.class),
                                            "application_1_0001",
                                            driver)));
        }
        assertArrayEquals(new Throwable[] {closeFailure}, startupFailure.getSuppressed());
        verify(driver).close();
    }

    @Test
    void stopsMasterAndPreservesCoordinatorFailureWhenDriverCleanupFails() throws Exception {
        SeaTunnelServer server = mock(SeaTunnelServer.class);
        HazelcastInstanceImpl master = masterWithServer(server);
        ResourceManagerDriver<?> driver = mock(ResourceManagerDriver.class);
        InterruptedException interrupted = new InterruptedException("coordinator activation");
        IllegalStateException driverFailure = new IllegalStateException("driver close");
        AssertionError masterFailure = new AssertionError("master shutdown");
        when(server.awaitCoordinatorService(anyLong())).thenThrow(interrupted);
        doThrow(driverFailure).when(driver).close();
        doThrow(masterFailure).when(master).shutdown();

        assertSame(
                interrupted,
                assertThrows(
                        InterruptedException.class,
                        () ->
                                SeatunnelYarnApplicationCli.runJob(
                                        mock(ApplicationSpecification.class), master, driver)));
        assertArrayEquals(
                new Throwable[] {driverFailure, masterFailure}, interrupted.getSuppressed());
        verify(driver).close();
        verify(master).shutdown();
        verify(server, never()).getCoordinatorService();
    }

    @Test
    void leavesAnInitializedResourceManagerInChargeOfItsDriver() throws Exception {
        SeaTunnelServer server = mock(SeaTunnelServer.class);
        HazelcastInstanceImpl master = masterWithServer(server);
        ResourceManagerDriver<?> driver = mock(ResourceManagerDriver.class);
        LinkageError failure = new LinkageError("job startup");
        when(server.awaitCoordinatorService(anyLong())).thenThrow(failure);
        when(server.hasInitializedResourceManager()).thenReturn(true);

        assertSame(
                failure,
                assertThrows(
                        LinkageError.class,
                        () ->
                                SeatunnelYarnApplicationCli.runJob(
                                        mock(ApplicationSpecification.class), master, driver)));
        verify(driver, never()).close();
        verify(master).shutdown();
    }

    private static HazelcastInstanceImpl masterWithServer(SeaTunnelServer server)
            throws ReflectiveOperationException {
        HazelcastInstanceImpl master = mock(HazelcastInstanceImpl.class);
        Node node = mock(Node.class);
        NodeEngineImpl engine = mock(NodeEngineImpl.class);
        when(node.getNodeEngine()).thenReturn(engine);
        when(engine.getService(SeaTunnelServer.SERVICE_NAME)).thenReturn(server);
        // Hazelcast exposes its node as a final field rather than through an accessor.
        Field field = HazelcastInstanceImpl.class.getField("node");
        field.setAccessible(true);
        field.set(master, node);
        return master;
    }
}

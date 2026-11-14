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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ServerExecuteCommandTest {

    @Test
    @EnabledOnJre(value = {JRE.JAVA_11, JRE.JAVA_17})
    public void testSupportedJavaVersionCheck() {
        Assertions.assertFalse(ServerExecuteCommand.isUnsupportedJavaVersion());
    }

    @Test
    public void testApplyClusterRole() {
        // master
        SeaTunnelConfig masterConfig = new SeaTunnelConfig();
        ServerExecuteCommand.applyClusterRole(masterConfig, "master");
        Assertions.assertEquals(
                EngineConfig.ClusterRole.MASTER, masterConfig.getEngineConfig().getClusterRole());
        Assertions.assertFalse(masterConfig.getHazelcastConfig().isLiteMember());

        // worker
        SeaTunnelConfig workerConfig = new SeaTunnelConfig();
        ServerExecuteCommand.applyClusterRole(workerConfig, "worker");
        Assertions.assertEquals(
                EngineConfig.ClusterRole.WORKER, workerConfig.getEngineConfig().getClusterRole());
        Assertions.assertTrue(workerConfig.getHazelcastConfig().isLiteMember());

        // master_and_worker
        SeaTunnelConfig masterAndWorkerConfig = new SeaTunnelConfig();
        ServerExecuteCommand.applyClusterRole(masterAndWorkerConfig, "master_and_worker");
        Assertions.assertEquals(
                EngineConfig.ClusterRole.MASTER_AND_WORKER,
                masterAndWorkerConfig.getEngineConfig().getClusterRole());
        Assertions.assertFalse(masterAndWorkerConfig.getHazelcastConfig().isLiteMember());

        // case-insensitive
        SeaTunnelConfig caseInsensitiveConfig = new SeaTunnelConfig();
        ServerExecuteCommand.applyClusterRole(caseInsensitiveConfig, "Master_And_Worker");
        Assertions.assertEquals(
                EngineConfig.ClusterRole.MASTER_AND_WORKER,
                caseInsensitiveConfig.getEngineConfig().getClusterRole());

        // invalid value
        SeaTunnelConfig invalidConfig = new SeaTunnelConfig();
        Assertions.assertThrows(
                SeaTunnelEngineException.class,
                () -> ServerExecuteCommand.applyClusterRole(invalidConfig, "invalid_role"));
    }

    @Test
    public void testMemberList() throws InterruptedException {
        String clusterName = getClusterName("ServerExecuteCommandTest");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);

        List<HazelcastInstanceImpl> instances = new ArrayList<>();
        try {
            instances.add(SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig));
            instances.add(SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig));
            instances.add(SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig));
            instances.add(SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig));
            instances.add(SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig));

            HazelcastInstanceImpl firstInstance = instances.get(0);
            long deadline = System.currentTimeMillis() + 30_000;
            while (firstInstance.getCluster().getMembers().size() < 5) {
                if (System.currentTimeMillis() > deadline) {
                    Assertions.fail(
                            "Cluster did not form within 30s, members: "
                                    + firstInstance.getCluster().getMembers().size());
                }
                Thread.sleep(500);
            }

            ServerCommandArgs serverCommandArgs = new ServerCommandArgs();
            serverCommandArgs.setClusterName(clusterName);
            serverCommandArgs.setShowClusterMembers(true);

            ServerExecuteCommand serverExecuteCommand = new ServerExecuteCommand(serverCommandArgs);
            Set<Member> members = serverExecuteCommand.showClusterMembers();
            Assertions.assertEquals(5, members.size());
        } finally {
            // Stop the workers before the masters. A worker that loses its last master terminates
            // itself ("All node is lite node, shutdown this cluster"). When that self-termination
            // races with a graceful shutdown() issued from here, the graceful shutdown wins the
            // state transition and then waits for a master that no longer exists, so it never
            // returns and the test (and the whole unit-test job) hangs until the CI timeout.
            // Shutting down in reverse creation order keeps a master alive for every worker.
            for (int i = instances.size() - 1; i >= 0; i--) {
                try {
                    instances.get(i).shutdown();
                } catch (Exception ignored) {
                }
            }
        }
    }

    public static String getClusterName(String testClassName) {
        return System.getProperty("user.name") + "_" + testClassName;
    }
}

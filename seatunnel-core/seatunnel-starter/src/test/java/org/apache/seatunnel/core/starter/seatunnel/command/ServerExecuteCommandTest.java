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
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ServerExecuteCommandTest {

    @Test
    @DisabledOnJre(value = JRE.JAVA_11, disabledReason = "the test case only works on Java 8")
    public void testJavaVersionCheck() {
        String realVersion = System.getProperty("java.version");
        try {
            System.setProperty("java.version", "1.8.0_191");
            Assertions.assertFalse(ServerExecuteCommand.isAllocatingThreadGetName());
            System.setProperty("java.version", "1.8.0_60");
            Assertions.assertTrue(ServerExecuteCommand.isAllocatingThreadGetName());
        } finally {
            System.setProperty("java.version", realVersion);
        }
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
            for (HazelcastInstanceImpl inst : instances) {
                try {
                    inst.shutdown();
                } catch (Exception ignored) {
                }
            }
        }
    }

    public static String getClusterName(String testClassName) {
        return System.getProperty("user.name") + "_" + testClassName;
    }
}

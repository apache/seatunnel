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

import java.util.Set;

public class ServerExecuteCommandTest {

    @Test
    @DisabledOnJre(value = JRE.JAVA_11, disabledReason = "the test case only works on Java 8")
    public void testJavaVersionCheck() {
        String realVersion = System.getProperty("java.version");
        System.setProperty("java.version", "1.8.0_191");
        Assertions.assertFalse(ServerExecuteCommand.isAllocatingThreadGetName());
        System.setProperty("java.version", "1.8.0_60");
        Assertions.assertTrue(ServerExecuteCommand.isAllocatingThreadGetName());
        System.setProperty("java.version", realVersion);
    }

    @Test
    public void testMemberList() {
        String clusterName = getClusterName("ServerExecuteCommandTest");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);

        SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);
        SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);
        SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
        SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
        SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);

        ServerCommandArgs serverCommandArgs = new ServerCommandArgs();
        serverCommandArgs.setClusterName(clusterName);
        serverCommandArgs.setShowClusterMembers(true);

        ServerExecuteCommand serverExecuteCommand = new ServerExecuteCommand(serverCommandArgs);
        Set<Member> members = serverExecuteCommand.showClusterMembers();
        Assertions.assertEquals(5, members.size());
    }

    public static String getClusterName(String testClassName) {
        return System.getProperty("user.name") + "_" + testClassName;
    }
}

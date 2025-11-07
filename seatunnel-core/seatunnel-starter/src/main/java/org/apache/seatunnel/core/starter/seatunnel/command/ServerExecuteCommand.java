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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This command is used to execute the SeaTunnel engine job by SeaTunnel API. */
@Slf4j
public class ServerExecuteCommand implements Command<ServerCommandArgs> {

    private final ServerCommandArgs serverCommandArgs;

    public ServerExecuteCommand(ServerCommandArgs serverCommandArgs) {
        this.serverCommandArgs = serverCommandArgs;
    }

    @Override
    public void execute() {
        checkEnvironment();
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        if (this.serverCommandArgs.isShowClusterMembers()) {
            showClusterMembers();
            return;
        }

        String clusterRole = this.serverCommandArgs.getClusterRole();
        if (StringUtils.isNotBlank(clusterRole)) {
            if (EngineConfig.ClusterRole.MASTER.toString().equalsIgnoreCase(clusterRole)) {
                seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER);
            } else if (EngineConfig.ClusterRole.WORKER.toString().equalsIgnoreCase(clusterRole)) {
                seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.WORKER);

                // in hazelcast lite node will not store IMap data.
                seaTunnelConfig.getHazelcastConfig().setLiteMember(true);
            } else {
                throw new SeaTunnelEngineException("Not supported cluster role: " + clusterRole);
            }
        } else {
            seaTunnelConfig
                    .getEngineConfig()
                    .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        }

        SeaTunnelServerStarter.createHazelcastInstance(
                seaTunnelConfig, Thread.currentThread().getName());
    }

    private void checkEnvironment() {
        if (isAllocatingThreadGetName()) {
            log.warn(
                    "The current JDK version is not recommended. Please upgrade to JDK 1.8.0_102 or higher. "
                            + "The current version will affect the performance of log printing. "
                            + "For details, please refer to https://issues.apache.org/jira/browse/LOG4J2-2052");
        }
    }

    static boolean isAllocatingThreadGetName() {
        // LOG4J2-2052, LOG4J2-2635 JDK 8u102 ("1.8.0_102") removed the String allocation in
        // Thread.getName()
        if (SystemUtils.IS_JAVA_1_8) {
            try {
                Pattern javaVersionPattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)_(\\d+)");
                Matcher m = javaVersionPattern.matcher(System.getProperty("java.version"));
                if (m.matches()) {
                    return Integer.parseInt(m.group(3)) == 0 && Integer.parseInt(m.group(4)) < 102;
                }
                return true;
            } catch (Exception e) {
                return true;
            }
        } else {
            return !SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8);
        }
    }

    @VisibleForTesting
    public Set<Member> showClusterMembers() {
        HazelcastClientInstanceImpl client = null;
        try {
            String clusterName = serverCommandArgs.getClusterName();
            if (StringUtils.isBlank(clusterName)) {
                throw new SeaTunnelEngineException(
                        "Cluster name is required. Please specify it using -cn or --cluster option.");
            }
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            client =
                    ((HazelcastClientProxy) HazelcastClient.newHazelcastClient(clientConfig))
                            .client;
            if (!client.getLifecycleService().isRunning()) {
                throw new SeaTunnelEngineException(
                        String.format(
                                "cluster: %s is not running, Please start the cluster first.",
                                clusterName));
            }
            Set<Member> members = client.getCluster().getMembers();
            if (members.isEmpty()) {
                System.out.println("No active members found in the cluster.");
                return members;
            }

            Collection<Member> memberList = client.getClientClusterService().getMemberList();

            Member masterMember = client.getClientClusterService().getMasterMember();
            System.out.printf(
                    "%-36s %-20s %-20s %-10s\n", "Member ID", "Address", "Role", "Version");

            for (Member member : members) {
                System.out.printf(
                        "%-36s %-20s %-20s %-10s\n",
                        member.getUuid(),
                        member.getAddress(),
                        getRole(masterMember.getAddress(), member),
                        member.getVersion());
            }
            return members;
        } catch (Exception e) {
            throw new SeaTunnelEngineException("Failed to get cluster members information", e);
        } finally {
            if (client != null) {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    log.warn("Failed to shutdown Hazelcast client", e);
                }
            }
        }
    }

    private String getRole(Address masterAddress, Member member) {

        if (member.isLiteMember()) {
            return EngineConfig.ClusterRole.WORKER.toString();
        }
        if (masterAddress.toString().equals(member.getAddress().toString())) {
            return "ACTIVE MASTER";
        }
        return EngineConfig.ClusterRole.MASTER.toString();
    }
}

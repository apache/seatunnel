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
import org.apache.seatunnel.shade.org.apache.commons.lang3.JavaVersion;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.SystemUtils;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Set;

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
            applyClusterRole(seaTunnelConfig, clusterRole);
        } else {
            seaTunnelConfig
                    .getEngineConfig()
                    .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        }

        SeaTunnelServerStarter.createHazelcastInstance(
                seaTunnelConfig, Thread.currentThread().getName());
    }

    /**
     * Applies the cluster role to the SeaTunnel config based on the provided role string. Supports
     * master, worker and master_and_worker (case-insensitive).
     *
     * @param seaTunnelConfig the SeaTunnel config to configure
     * @param clusterRole the cluster role string
     */
    @VisibleForTesting
    static void applyClusterRole(SeaTunnelConfig seaTunnelConfig, String clusterRole) {
        if (EngineConfig.ClusterRole.MASTER.toString().equalsIgnoreCase(clusterRole)) {
            seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER);
        } else if (EngineConfig.ClusterRole.WORKER.toString().equalsIgnoreCase(clusterRole)) {
            seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.WORKER);

            // in hazelcast lite node will not store IMap data.
            seaTunnelConfig.getHazelcastConfig().setLiteMember(true);
        } else if (EngineConfig.ClusterRole.MASTER_AND_WORKER
                .toString()
                .equalsIgnoreCase(clusterRole)) {
            seaTunnelConfig
                    .getEngineConfig()
                    .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        } else {
            throw new SeaTunnelEngineException("Not supported cluster role: " + clusterRole);
        }
    }

    private void checkEnvironment() {
        if (isUnsupportedJavaVersion()) {
            log.warn(
                    "The current JDK version is not supported. "
                            + "Please use JDK 11 or JDK 17 to run SeaTunnel.");
        }
    }

    static boolean isUnsupportedJavaVersion() {
        return !SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_11);
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
            Address activeMasterAddress = getActiveMasterAddress(memberList, masterMember);
            System.out.printf(
                    "%-36s %-20s %-20s %-10s\n", "Member ID", "Address", "Role", "Version");

            for (Member member : members) {
                System.out.printf(
                        "%-36s %-20s %-20s %-10s\n",
                        member.getUuid(),
                        member.getAddress(),
                        getRole(activeMasterAddress, member),
                        member.getVersion());
            }
            String activeMasterNote =
                    describeActiveMasterResolution(masterMember, activeMasterAddress);
            if (activeMasterNote != null) {
                System.out.println(activeMasterNote);
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

    /**
     * Resolves the active coordinator shown by the member-list command.
     *
     * @param memberList current cluster members
     * @param masterMember Hazelcast master member, or {@code null} when it is unknown
     * @return active coordinator address, or {@code null} when no coordinator is known
     */
    Address getActiveMasterAddress(Collection<Member> memberList, Member masterMember) {
        if (masterMember == null) {
            return null;
        }
        if (!masterMember.isLiteMember()) {
            return masterMember.getAddress();
        }
        return memberList.stream()
                .filter(member -> !member.isLiteMember())
                .map(Member::getAddress)
                .findFirst()
                .orElse(null);
    }

    /**
     * Explains how far the {@code ACTIVE MASTER} label of the member list can be trusted.
     *
     * <p>The command only sees the client-side membership view. When Hazelcast mastership sits on a
     * worker-only member, the active coordinator is inferred with the same rule the server applies
     * (the first coordinator-capable member in membership order), so the label is best effort and
     * can lag behind the cluster during failover. When nothing can be inferred, operators must be
     * told explicitly, otherwise plain {@code MASTER} rows read as if no election were pending.
     *
     * @param masterMember Hazelcast master member, or {@code null} when it is unknown
     * @param activeMasterAddress coordinator resolved for the member list, or {@code null} when no
     *     coordinator is known
     * @return note printed below the member list, or {@code null} when the label is authoritative
     */
    String describeActiveMasterResolution(Member masterMember, Address activeMasterAddress) {
        if (activeMasterAddress == null) {
            return "Active master: UNKNOWN. No coordinator-capable member can be resolved from the "
                    + "current membership view, so MASTER rows only show the configured role.";
        }
        if (masterMember != null && masterMember.isLiteMember()) {
            return String.format(
                    "Active master: %s (best effort). Hazelcast master %s is a worker-only member, "
                            + "so the coordinator is inferred on the client with the server rule "
                            + "and can lag behind the cluster during failover.",
                    activeMasterAddress, masterMember.getAddress());
        }
        return null;
    }

    private String getRole(Address masterAddress, Member member) {

        if (member.isLiteMember()) {
            return EngineConfig.ClusterRole.WORKER.toString();
        }
        if (masterAddress != null
                && masterAddress.toString().equals(member.getAddress().toString())) {
            return "ACTIVE MASTER";
        }
        return EngineConfig.ClusterRole.MASTER.toString();
    }
}

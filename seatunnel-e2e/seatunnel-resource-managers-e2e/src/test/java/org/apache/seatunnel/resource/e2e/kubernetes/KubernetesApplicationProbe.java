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

package org.apache.seatunnel.resource.e2e.kubernetes;

import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Test-only in-cluster client: every request bootstraps through the stable application Service. */
public final class KubernetesApplicationProbe {
    private KubernetesApplicationProbe() {}

    public static void main(String[] args) throws Exception {

        ClientConfig config = ConfigProvider.locateAndGetClientConfig();
        config.setClusterName(SeatunnelApplicationConfig.clusterName(args[0]));
        config.getNetworkConfig()
                .setAddresses(
                        Collections.singletonList(
                                (args.length > 4 ? args[4] : args[0]) + ":" + args[2]))
                .setSmartRouting(false);
        config.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(20000);
        SeaTunnelHazelcastClient client = new SeaTunnelHazelcastClient(config);
        try {

            if (args.length > 3 && "members".equals(args[3])) {
                List<Member> members =
                        new ArrayList<>(client.getHazelcastInstance().getCluster().getMembers());
                System.out.println(
                        "HA_MEMBERS "
                                + members.stream().filter(m -> !m.isLiteMember()).count()
                                + " "
                                + members.stream()
                                        .filter(m -> !m.isLiteMember())
                                        .findFirst()
                                        .map(m -> resolvedHost(m.getAddress()))
                                        .orElse("none"));
                return;
            }
            JobClient jobs = new JobClient(client);
            long job = Long.parseLong(args[1]);
            if (args.length > 3 && "cancel".equals(args[3])) {
                jobs.getJobProxy(job).cancelJob();
                System.out.println("HA_CANCEL_ACK " + job);
                return;
            }
            CheckpointOverview overview = jobs.getCheckpointOverview(job);
            long checkpoint =
                    overview.getPipelines().values().stream()
                            .filter(p -> p.getLatestCompleted() != null)
                            .mapToLong(p -> p.getLatestCompleted().getCheckpointId())
                            .max()
                            .orElse(-1);
            long restored =
                    overview.getPipelines().values().stream()
                            .mapToLong(p -> p.getCounts().getRestored())
                            .sum();
            List<Member> members =
                    new ArrayList<>(client.getHazelcastInstance().getCluster().getMembers());
            String active =
                    members.stream()
                            .filter(m -> !m.isLiteMember())
                            .findFirst()
                            .get()
                            .getAddress()
                            .getInetAddress()
                            .getHostAddress();
            System.out.println(
                    "HA_PROBE "
                            + jobs.getJobProxy(job).getJobStatus()
                            + " "
                            + checkpoint
                            + " "
                            + restored
                            + " "
                            + active
                            + " "
                            + members.stream()
                                    .map(m -> resolvedHost(m.getAddress()))
                                    .collect(Collectors.joining(","))
                            + " "
                            + members.stream()
                                    .map(m -> m.getUuid().toString())
                                    .collect(Collectors.joining(","))
                            + " "
                            + jobs.getJobInfo(job).getJobId()
                            + " "
                            + members.stream().filter(m -> !m.isLiteMember()).count());
        } finally {
            client.getHazelcastInstance().shutdown();
        }
    }

    private static String resolvedHost(Address address) {
        try {
            return address.getInetAddress().getHostAddress();
        } catch (UnknownHostException failure) {
            throw new IllegalStateException("Native member address cannot resolve", failure);
        }
    }
}

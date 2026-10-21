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
import com.hazelcast.cluster.Member;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
            if (args.length > 3 && "persistent".equals(args[3])) {
                java.nio.file.Path prefix =
                        java.nio.file.Paths.get("/checkpoints/ha-client-" + job);
                java.nio.file.Path state = java.nio.file.Paths.get(prefix + ".state");
                java.nio.file.Path command = java.nio.file.Paths.get(prefix + ".command");
                String originalMaster =
                        client.getHazelcastInstance().getCluster().getMembers().stream()
                                .filter(m -> !m.isLiteMember())
                                .findFirst()
                                .get()
                                .getUuid()
                                .toString();
                java.util.UUID clientIdentity = java.util.UUID.randomUUID();
                java.nio.file.Files.writeString(state, "READY " + clientIdentity);
                long deadline =
                        System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(8);
                boolean recovered = false;
                while (System.nanoTime() < deadline) {
                    try {
                        if (jobs.getJobProxy(job).getJobStatus()
                                == org.apache.seatunnel.engine.common.job.JobStatus.RUNNING) {
                            String active =
                                    client.getHazelcastInstance().getCluster().getMembers().stream()
                                            .filter(m -> !m.isLiteMember())
                                            .findFirst()
                                            .get()
                                            .getUuid()
                                            .toString();
                            if (!originalMaster.equals(active) && !recovered) {
                                System.out.println(
                                        "HA_PERSISTENT_RECOVERED " + clientIdentity + " " + active);
                                recovered = true;
                            }
                        }
                        if (java.nio.file.Files.exists(command)
                                && "cancel".equals(java.nio.file.Files.readString(command))) {
                            if (!recovered)
                                throw new IllegalStateException(
                                        "Persistent client never observed promoted master");
                            jobs.getJobProxy(job).cancelJob();
                            System.out.println("HA_PERSISTENT_CANCELED " + clientIdentity);
                            return;
                        }
                    } catch (com.hazelcast.core.HazelcastInstanceNotActiveException stopped) {
                        throw stopped;
                    } catch (Exception transientFailure) {
                        System.out.println(
                                "HA_PERSISTENT_RETRY "
                                        + transientFailure.getClass().getSimpleName());
                    }
                    Thread.sleep(1000);
                }
                throw new IllegalStateException(
                        "Persistent native client did not complete its recovery/cancel gate");
            }

            if (args.length > 3 && "checkpoint-read".equals(args[3])) {
                try {
                    int count =
                            jobs.getCheckpointData(
                                            job,
                                            org.apache.seatunnel.engine.core.job.RestoreMode
                                                    .CHECKPOINT)
                                    .size();
                    System.out.println("HA_CHECKPOINT_READ_OK " + job + " " + count);
                } catch (Exception failure) {
                    System.out.println("HA_CHECKPOINT_READ_FAILED " + job);
                    failure.printStackTrace(System.out);
                }
                return;
            }
            if (args.length > 3 && "cancel".equals(args[3])) {
                jobs.getJobProxy(job).cancelJob();
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
                                    .collect(java.util.stream.Collectors.joining(","))
                            + " "
                            + members.stream()
                                    .map(m -> m.getUuid().toString())
                                    .collect(java.util.stream.Collectors.joining(",")));
        } finally {
            client.getHazelcastInstance().shutdown();
        }
    }

    private static String resolvedHost(com.hazelcast.cluster.Address address) {
        try {
            return address.getInetAddress().getHostAddress();
        } catch (java.net.UnknownHostException failure) {
            throw new IllegalStateException("Native member address cannot resolve", failure);
        }
    }
}

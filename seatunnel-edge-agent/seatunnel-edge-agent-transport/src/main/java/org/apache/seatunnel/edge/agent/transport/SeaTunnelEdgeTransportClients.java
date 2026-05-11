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

package org.apache.seatunnel.edge.agent.transport;

import org.apache.seatunnel.engine.client.SeaTunnelClient;

import com.hazelcast.client.config.ClientConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Factory helpers for running {@link EdgeTransportClient} against Zeta: builds {@link
 * SeaTunnelClient} from cluster member addresses and derives {@link JobTaskGroupAddressesLookup}
 * from {@link SeaTunnelClient#getJobTaskGroupAddresses(Long)}.
 */
public final class SeaTunnelEdgeTransportClients {

    private SeaTunnelEdgeTransportClients() {}

    /**
     * Returns a lookup backed by {@link SeaTunnelClient#getJobTaskGroupAddresses(Long)}. RPC /
     * Hazelcast failures are surfaced as {@link IOException}.
     */
    public static JobTaskGroupAddressesLookup jobTaskGroupAddressesLookup(
            SeaTunnelClient seaTunnelClient) {
        Objects.requireNonNull(seaTunnelClient, "seaTunnelClient");
        return jobId -> {
            try {
                String json = seaTunnelClient.getJobTaskGroupAddresses(jobId);
                if (json == null) {
                    throw new IOException(
                            "getJobTaskGroupAddresses returned null for jobId=" + jobId);
                }
                return json;
            } catch (RuntimeException ex) {
                throw new IOException(
                        "SeaTunnelClient.getJobTaskGroupAddresses failed for jobId=" + jobId, ex);
            }
        };
    }

    /**
     * Preferred wiring: Zeta discovery + EdgeSocket transport share one {@link SeaTunnelClient}.
     */
    public static EdgeTransportClient newEdgeTransportClient(
            EdgeTransportConfig transportConfig, SeaTunnelClient seaTunnelClient) {
        return new EdgeTransportClient(
                transportConfig, jobTaskGroupAddressesLookup(seaTunnelClient));
    }

    /**
     * Sets cluster name and replaces client network addresses with {@code memberAddresses}. Same
     * shape as {@code hazelcast-client.yaml} {@code network.cluster-members}.
     *
     * <p>Each entry must be {@code host} or {@code host:port} as accepted by Hazelcast.
     */
    public static ClientConfig configureClusterNetwork(
            ClientConfig clientConfig, String clusterName, Collection<String> memberAddresses) {
        Objects.requireNonNull(clientConfig, "clientConfig");
        Objects.requireNonNull(clusterName, "clusterName");
        Objects.requireNonNull(memberAddresses, "memberAddresses");
        List<String> normalized = normalizeMemberAddresses(memberAddresses);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("memberAddresses is empty");
        }
        clientConfig.setClusterName(clusterName);
        clientConfig.getNetworkConfig().setAddresses(normalized);
        return clientConfig;
    }

    /** Empty {@link ClientConfig} with cluster name and static member list. */
    public static ClientConfig newHazelcastClientConfig(
            String clusterName, Collection<String> memberAddresses) {
        return configureClusterNetwork(new ClientConfig(), clusterName, memberAddresses);
    }

    public static SeaTunnelClient newSeaTunnelClient(
            String clusterName, Collection<String> memberAddresses) {
        return new SeaTunnelClient(newHazelcastClientConfig(clusterName, memberAddresses));
    }

    public static SeaTunnelClient newSeaTunnelClient(
            String clusterName, String... memberAddresses) {
        Objects.requireNonNull(memberAddresses, "memberAddresses");
        List<String> list = new ArrayList<>(memberAddresses.length);
        for (String a : memberAddresses) {
            list.add(a);
        }
        return newSeaTunnelClient(clusterName, list);
    }

    /**
     * Split {@code commaSeparated} on commas, trim segments, then {@link
     * #normalizeMemberAddresses}.
     */
    public static List<String> normalizeMemberAddressesFromCsv(String commaSeparated) {
        Objects.requireNonNull(commaSeparated, "commaSeparated");
        String[] parts = commaSeparated.split(",");
        List<String> raw = new ArrayList<>(parts.length);
        for (String p : parts) {
            raw.add(p.trim());
        }
        return normalizeMemberAddresses(raw);
    }

    /** Trim, drop blanks, dedupe while preserving first-seen order. */
    public static List<String> normalizeMemberAddresses(Collection<String> raw) {
        Objects.requireNonNull(raw, "raw");
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String s : raw) {
            if (s == null) {
                continue;
            }
            String t = s.trim();
            if (!t.isEmpty()) {
                set.add(t);
            }
        }
        return new ArrayList<>(set);
    }
}

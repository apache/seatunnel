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

import org.apache.seatunnel.edge.agent.config.AgentYamlConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Maps validated YAML {@code output} to EdgeSocket transport primitives (cluster hosts + config).
 */
public final class EdgeTransportConfigFactory {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private EdgeTransportConfigFactory() {}

    /**
     * Maps validated {@link AgentYamlConfig.OutputDefinition} to {@link EdgeTransportConfig}
     * defaults (timeouts align with prior Edge agent defaults).
     */
    public static EdgeTransportConfig toEdgeTransportConfig(AgentYamlConfig.OutputDefinition out) {
        Objects.requireNonNull(out, "out");
        EdgeTransportConfig.Builder b =
                EdgeTransportConfig.builder()
                        .jobId(out.getJobId())
                        .authToken(out.getAuthToken())
                        .edgeIngressPort(out.getPort());
        if (out.getConnectTimeoutMs() != null) {
            b.connectTimeoutMs(out.getConnectTimeoutMs());
        }
        if (out.getReadTimeoutMs() != null) {
            b.readTimeoutMs(out.getReadTimeoutMs());
        }
        return b.build();
    }

    /**
     * Treats configured hosts as the distinct ingress set (same JSON shape as {@code
     * getJobTaskGroupAddresses}, {@code host} field only).
     */
    public static JobTaskGroupAddressesLookup staticClusterHostsLookup(List<String> hosts) {
        Objects.requireNonNull(hosts, "hosts");
        List<String> copy = new ArrayList<>(hosts);
        return jobId -> hostsJsonForLookup(copy);
    }

    private static String hostsJsonForLookup(List<String> hosts) throws IOException {
        List<Map<String, String>> rows = new ArrayList<>(hosts.size());
        for (String host : hosts) {
            Map<String, String> row = new HashMap<>();
            row.put("host", host);
            rows.add(row);
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(rows);
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to serialize cluster-addresses for discovery", e);
        }
    }
}

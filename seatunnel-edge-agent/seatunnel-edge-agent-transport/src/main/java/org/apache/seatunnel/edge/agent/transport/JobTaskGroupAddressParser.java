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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Parses JSON returned by {@link
 * org.apache.seatunnel.engine.client.job.JobClient#getJobTaskGroupAddresses(Long)} (surfaced as
 * {@link org.apache.seatunnel.engine.client.SeaTunnelClient#getJobTaskGroupAddresses(Long)}).
 *
 * <p>The payload is a JSON array of objects containing at least {@code host}. The {@code port}
 * field reflects the Hazelcast member port, not the EdgeSocket ingress port; callers must supply
 * the configured ingress port separately when building socket addresses.
 */
public final class JobTaskGroupAddressParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static List<String> parseDistinctHosts(String json) throws IOException {
        Objects.requireNonNull(json, "json");
        JsonNode root = OBJECT_MAPPER.readTree(json);
        if (!root.isArray()) {
            throw new IOException("Expected JSON array for task group addresses");
        }
        LinkedHashSet<String> hosts = new LinkedHashSet<>();
        for (JsonNode node : root) {
            if (node == null || !node.isObject()) {
                continue;
            }
            JsonNode hostNode = node.get("host");
            if (hostNode == null || !hostNode.isTextual()) {
                continue;
            }
            String host = hostNode.asText().trim();
            if (!host.isEmpty()) {
                hosts.add(host);
            }
        }
        return new ArrayList<>(hosts);
    }

    private JobTaskGroupAddressParser() {}
}

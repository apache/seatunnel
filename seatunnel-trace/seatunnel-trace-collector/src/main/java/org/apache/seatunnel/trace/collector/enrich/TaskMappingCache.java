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

package org.apache.seatunnel.trace.collector.enrich;

import org.apache.seatunnel.trace.collector.config.TraceCollectorConfig;
import org.apache.seatunnel.trace.collector.http.TraceHttpServer;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TaskMappingCache {

    @Value
    public static class Mapping {
        String worker;
        String taskGroupName;
        String taskClass;
    }

    private final String urlTemplate;
    private final String token;
    private final long ttlMs;
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public TaskMappingCache(TraceCollectorConfig config) {
        this.urlTemplate = config.getEngineTaskMappingUrlTemplate();
        this.token = config.getEngineTaskMappingToken();
        this.ttlMs = config.getEngineTaskMappingCacheTtlMs();
    }

    public boolean isEnabled() {
        return urlTemplate != null && urlTemplate.contains("{jobId}");
    }

    public Map<Long, Mapping> getMapping(String jobId) {
        if (!isEnabled() || jobId == null || jobId.isEmpty()) {
            return Collections.emptyMap();
        }
        long now = System.currentTimeMillis();
        CacheEntry current = cache.get(jobId);
        if (current != null && (now - current.loadedAtMs) <= ttlMs) {
            return current.mapping;
        }
        return cache.compute(
                        jobId,
                        (k, old) -> {
                            long now2 = System.currentTimeMillis();
                            if (old != null && (now2 - old.loadedAtMs) <= ttlMs) {
                                return old;
                            }
                            Map<Long, Mapping> mapping = fetch(jobId);
                            if (mapping == null) {
                                if (old != null) {
                                    return old;
                                }
                                mapping = Collections.emptyMap();
                            }
                            return new CacheEntry(now2, mapping);
                        })
                .mapping;
    }

    private Map<Long, Mapping> fetch(String jobId) {
        String url = urlTemplate.replace("{jobId}", jobId);
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(5000);
            if (token != null && !token.isEmpty()) {
                conn.setRequestProperty("X-Seatunnel-Token", token);
            }
            int code = conn.getResponseCode();
            if (code == 404) {
                return Collections.emptyMap();
            }
            if (code / 100 != 2) {
                log.debug("Fetch task mapping failed, code={}, url={}", code, url);
                return null;
            }
            try (InputStream in = conn.getInputStream()) {
                JsonNode root = TraceHttpServer.MAPPER.readTree(in);
                JsonNode items = root == null ? null : root.get("items");
                if (items == null || !items.isArray()) {
                    return Collections.emptyMap();
                }
                Map<Long, Mapping> out = new HashMap<>();
                for (JsonNode item : items) {
                    Long taskId = asLongOrNull(item.get("taskId"));
                    if (taskId == null) {
                        continue;
                    }
                    out.put(
                            taskId,
                            new Mapping(
                                    asText(item.get("worker")),
                                    asText(item.get("taskGroupName")),
                                    asText(item.get("taskClass"))));
                }
                return out;
            }
        } catch (Exception e) {
            log.debug("Fetch task mapping failed, url={}", url, e);
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static String asText(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.isTextual() ? node.asText() : node.toString();
    }

    private static Long asLongOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            String s = node.asText();
            if (s == null || s.isEmpty()) {
                return null;
            }
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static class CacheEntry {
        private final long loadedAtMs;
        private final Map<Long, Mapping> mapping;

        private CacheEntry(long loadedAtMs, Map<Long, Mapping> mapping) {
            this.loadedAtMs = loadedAtMs;
            this.mapping = mapping;
        }
    }
}

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

package org.apache.seatunnel.trace.collector.config;

import lombok.Getter;
import lombok.ToString;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Getter
@ToString
public class TraceCollectorConfig {
    private final int serverPort;
    private final int maxRequestBytes;
    private final String authToken;

    private final String dbType;
    private final String jdbcUrl;
    private final String jdbcUsername;
    private final String jdbcPassword;
    private final String dbSchema;

    private final boolean parsePayloadEnabled;
    private final String engineTaskMappingUrlTemplate;
    private final String engineTaskMappingToken;
    private final long engineTaskMappingCacheTtlMs;

    private TraceCollectorConfig(Properties p) {
        this.serverPort = intOrDefault(p, "server.port", 9808);
        this.maxRequestBytes = intOrDefault(p, "server.maxRequestBytes", 10 * 1024 * 1024);
        this.authToken = trimToNull(p.getProperty("auth.token"));

        this.dbType = trimToNull(p.getProperty("db.type", "postgres"));
        this.jdbcUrl = required(p, "db.jdbcUrl");
        this.jdbcUsername = p.getProperty("db.username", "");
        this.jdbcPassword = p.getProperty("db.password", "");
        String schema = trimToNull(p.getProperty("db.schema"));
        if (schema == null) {
            schema = "postgres".equalsIgnoreCase(dbType) ? "public" : "";
        }
        this.dbSchema = schema;

        this.parsePayloadEnabled = boolOrDefault(p, "trace.parsePayload", true);
        this.engineTaskMappingUrlTemplate =
                trimToNull(p.getProperty("engine.taskMappingUrlTemplate"));
        this.engineTaskMappingToken = trimToNull(p.getProperty("engine.taskMappingToken"));
        this.engineTaskMappingCacheTtlMs =
                longOrDefault(p, "engine.taskMappingCacheTtlMs", 30_000L);
    }

    public static TraceCollectorConfig load() throws IOException {
        String path = System.getProperty("trace.collector.config");
        if (path == null || path.trim().isEmpty()) {
            path = System.getenv("TRACE_COLLECTOR_CONFIG");
        }

        Properties p = new Properties();
        if (path == null || path.trim().isEmpty()) {
            try (InputStream in =
                    TraceCollectorConfig.class
                            .getClassLoader()
                            .getResourceAsStream("trace-collector.properties")) {
                if (in != null) {
                    p.load(in);
                }
            }
        } else {
            try (InputStream in = new FileInputStream(path)) {
                p.load(in);
            }
        }
        return new TraceCollectorConfig(p);
    }

    private static int intOrDefault(Properties p, String key, int defaultValue) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(v.trim());
    }

    private static boolean boolOrDefault(Properties p, String key, boolean defaultValue) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(v.trim());
    }

    private static long longOrDefault(Properties p, String key, long defaultValue) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return defaultValue;
        }
        return Long.parseLong(v.trim());
    }

    private static String required(Properties p, String key) {
        String v = trimToNull(p.getProperty(key));
        if (v == null) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return v;
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}

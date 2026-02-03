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

// by wjr 2025.10.23
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public final class PerformanceLogger {
    private static final Logger LOG = LoggerFactory.getLogger("JDBC_PERF");

    private PerformanceLogger() {}

    public static Span span(String stage) {
        return new Span(stage);
    }

    public static final class Span {
        private final String stage;
        private final long startNanos;
        private final Map<String, Object> fields = new LinkedHashMap<>();

        private Span(String stage) {
            this.stage = stage;
            this.startNanos = System.nanoTime();
        }

        public Span field(String key, Object value) {
            if (value != null) {
                fields.put(key, value);
            }
            return this;
        }

        public long end() {
            return end(null, null, null);
        }

        public long end(Long rows, Long bytes, String sql) {
            long durMs = (System.nanoTime() - startNanos) / 1_000_000L;
            StringBuilder sb = new StringBuilder("[JDBC-PERF] ");
            sb.append("stage=").append(stage);
            for (Map.Entry<String, Object> e : fields.entrySet()) {
                sb.append(" ").append(e.getKey()).append("=").append(e.getValue());
            }
            sb.append(" dur_ms=").append(durMs);
            if (rows != null) {
                sb.append(" rows=").append(rows);
            }
            if (bytes != null) {
                sb.append(" bytes=").append(bytes);
            }
            if (sql != null) {
                sb.append(" sql_hash=").append(sql.hashCode());
            }
            LOG.info(sb.toString());
            return durMs;
        }
    }
}

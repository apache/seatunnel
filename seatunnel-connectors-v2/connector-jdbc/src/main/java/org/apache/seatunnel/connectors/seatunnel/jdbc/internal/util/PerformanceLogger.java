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

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

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.observability.RealtimeMetricsService;

import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * REST entry point for active-master realtime observability snapshots.
 *
 * <p>The servlet only serves requests on the current master. It validates the realtime metrics
 * path, delegates aggregation reads to {@link RealtimeMetricsService}, and keeps the HTTP contract
 * small: job summaries, vertex busy windows, edge backpressure windows, and collector status.
 */
public class RealtimeMetricsServlet extends BaseServlet {

    private static final long DEFAULT_WINDOW_MS = TimeUnit.MINUTES.toMillis(3);
    private static final long MAX_WINDOW_MS = TimeUnit.MINUTES.toMillis(10);

    public RealtimeMetricsServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    /** Handles realtime metrics read endpoints under {@code /metrics/realtime}. */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        SeaTunnelServer server = getSeaTunnelServer(true);
        if (server == null) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not a master node");
            return;
        }
        RealtimeMetricsService service = server.getRealtimeMetricsService();
        if (service == null) {
            resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Realtime metrics disabled");
            return;
        }

        String path = req.getPathInfo();
        if (path == null || path.isEmpty() || "/".equals(path) || "/jobs".equals(path)) {
            Map<String, Object> out = new HashMap<>();
            out.put("jobs", service.listJobs());
            out.put("collector", service.getCollectorStatus());
            writeJson(resp, out);
            return;
        }

        String[] parts = path.split("/");
        // pathInfo starts with '/', e.g. /jobs/{jobId}/edges
        if (parts.length >= 3 && "jobs".equals(parts[1])) {
            long jobId;
            try {
                jobId = Long.parseLong(parts[2]);
            } catch (Exception e) {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid jobId");
                return;
            }

            if (parts.length == 4 && "edges".equals(parts[3])) {
                long windowMs =
                        clampWindowMs(parseLong(req.getParameter("windowMs"), DEFAULT_WINDOW_MS));
                writeJson(resp, service.getJobEdges(jobId, windowMs));
                return;
            }
            if (parts.length == 4 && "vertices".equals(parts[3])) {
                long windowMs =
                        clampWindowMs(parseLong(req.getParameter("windowMs"), DEFAULT_WINDOW_MS));
                writeJson(resp, service.getJobVertices(jobId, windowMs));
                return;
            }
        }

        resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Unsupported endpoint");
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    private static long clampWindowMs(long windowMs) {
        long w = Math.max(0L, windowMs);
        return Math.min(MAX_WINDOW_MS, w);
    }
}

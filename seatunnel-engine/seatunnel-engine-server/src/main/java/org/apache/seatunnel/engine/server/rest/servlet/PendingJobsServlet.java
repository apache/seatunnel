/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.engine.server.diagnostic.PendingJobsResponse;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.rest.service.PendingJobsService;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
public class PendingJobsServlet extends BaseServlet {

    private final PendingJobsService pendingJobsService;
    private static final Set<String> TIMESTAMP_FIELDS =
            new HashSet<>(
                    Arrays.asList(
                            "oldestEnqueueTimestamp",
                            "newestEnqueueTimestamp",
                            "enqueueTimestamp",
                            "checkTime"));
    private static final DateTimeFormatter PRETTY_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final Gson PRETTY_GSON = new GsonBuilder().setPrettyPrinting().create();

    public PendingJobsServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.pendingJobsService = new PendingJobsService(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        Map<String, String> params = new HashMap<>(getParameterMap(req));
        Long jobId = null;
        int limit = 0;
        boolean pretty = false;
        if (params.containsKey(RestConstant.JOB_ID)) {
            try {
                jobId = Long.parseLong(params.remove(RestConstant.JOB_ID));
            } catch (NumberFormatException e) {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid jobId");
                return;
            }
        }

        if (params.containsKey(RestConstant.LIMIT)) {
            try {
                limit = Integer.parseInt(params.remove(RestConstant.LIMIT));
            } catch (NumberFormatException e) {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid limit");
                return;
            }
        }

        if (params.containsKey(RestConstant.PRETTY)) {
            pretty = Boolean.parseBoolean(params.remove(RestConstant.PRETTY));
        }

        PendingJobsResponse response = pendingJobsService.getPendingJobs(params, jobId, limit);
        if (pretty) {
            writePrettyResponse(resp, response);
        } else {
            writeJson(resp, response);
        }
    }

    private void writePrettyResponse(HttpServletResponse resp, PendingJobsResponse response)
            throws IOException {
        JsonElement tree = PRETTY_GSON.toJsonTree(response);
        formatTimestampFields(tree);
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("application/json; charset=UTF-8");
        resp.getWriter().write(PRETTY_GSON.toJson(tree));
    }

    private void formatTimestampFields(JsonElement element) {
        if (element == null || element.isJsonNull()) {
            return;
        }
        if (element.isJsonObject()) {
            JsonObject object = element.getAsJsonObject();
            for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
                JsonElement value = entry.getValue();
                if (shouldFormatTimestamp(entry.getKey(), value)) {
                    long timestamp = value.getAsLong();
                    object.addProperty(entry.getKey(), formatTimestamp(timestamp));
                } else {
                    formatTimestampFields(value);
                }
            }
        } else if (element.isJsonArray()) {
            JsonArray array = element.getAsJsonArray();
            for (JsonElement child : array) {
                formatTimestampFields(child);
            }
        }
    }

    private boolean shouldFormatTimestamp(String key, JsonElement element) {
        if (!TIMESTAMP_FIELDS.contains(key) || element == null) {
            return false;
        }
        if (!element.isJsonPrimitive()) {
            return false;
        }
        JsonPrimitive primitive = element.getAsJsonPrimitive();
        return primitive.isNumber();
    }

    private String formatTimestamp(long timestamp) {
        return PRETTY_TIME_FORMATTER.format(Instant.ofEpochMilli(timestamp));
    }
}

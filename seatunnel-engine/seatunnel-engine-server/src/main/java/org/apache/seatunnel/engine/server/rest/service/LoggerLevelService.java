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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.log.LogLevels;
import org.apache.seatunnel.engine.server.operation.GetNodeHttpPortOperation;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.apache.logging.log4j.Level;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Reads and changes log levels of the node that serves the request, and optionally of every member
 * of the cluster. Runtime overrides are node local and are lost when a node restarts, so the
 * cluster scope is a convenience for the operator and not a replicated setting.
 */
@Slf4j
public class LoggerLevelService extends BaseService {

    public static final String SCOPE = "scope";
    public static final String SCOPE_CLUSTER = "cluster";
    public static final String SCOPE_NODE = "node";
    public static final String LEVEL = "level";

    private static final String NODE = "node";
    private static final String NODES = "nodes";
    private static final String LOGGERS = "loggers";
    private static final String NAME = "name";
    private static final String ORIGIN = "origin";
    private static final String FILE_LEVEL = "fileLevel";
    private static final String PREVIOUS_LEVEL = "previousLevel";
    private static final String STATUS = "status";
    private static final String ERROR = "error";
    private static final String SUCCESS = "SUCCESS";
    private static final String FAILURE = "FAILURE";
    private static final String PARTIAL_FAILURE = "PARTIAL_FAILURE";
    private static final String NO_OVERRIDE = "NO_OVERRIDE";
    private static final int REQUEST_TIMEOUT_MS = 5000;

    public LoggerLevelService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    /** Every logger of the running configuration of this node. */
    public JsonObject loggers() {
        JsonArray loggers = new JsonArray();
        for (Map.Entry<String, Level> logger : LogLevels.loggers().entrySet()) {
            loggers.add(loggerJson(logger.getKey(), logger.getValue()));
        }
        return new JsonObject().add(NODE, nodeId()).add(LOGGERS, loggers);
    }

    /** One logger of this node, resolved through its closest configured ancestor. */
    public JsonObject logger(String name) {
        return loggerJson(name, LogLevels.effectiveLevel(name)).add(NODE, nodeId());
    }

    /**
     * Overrides the level of one logger on this node. The level that was replaced comes from the
     * change itself; the rest of the answer is read afterwards, so a change another request makes
     * in between is already reflected in it.
     */
    public JsonObject setLevel(String name, Level level, String client) {
        Level previousLevel = LogLevels.apply(name, level);
        log.info(
                "Logger level changed: logger={}, {} -> {}, scope={}, client={}",
                name,
                previousLevel,
                level,
                SCOPE_NODE,
                client);
        return loggerJson(name, LogLevels.effectiveLevel(name))
                .add(NODE, nodeId())
                .add(PREVIOUS_LEVEL, previousLevel == null ? null : previousLevel.name())
                .add(STATUS, SUCCESS);
    }

    /** Reverts one logger of this node to the level it had before the first override. */
    public JsonObject resetLevel(String name, String client) {
        LogLevels.Reverted reverted = LogLevels.reset(name);
        Level previousLevel = reverted.getPreviousLevel();
        if (reverted.isReverted()) {
            log.info(
                    "Logger level reverted: logger={}, {} -> {}, scope={}, client={}",
                    name,
                    previousLevel,
                    reverted.getLevel(),
                    SCOPE_NODE,
                    client);
        }
        return loggerJson(name, reverted.getLevel())
                .add(NODE, nodeId())
                .add(PREVIOUS_LEVEL, previousLevel == null ? null : previousLevel.name())
                .add(STATUS, reverted.isReverted() ? SUCCESS : NO_OVERRIDE);
    }

    /** Every logger of every member of the cluster. */
    public JsonObject clusterLoggers(String name) {
        return fanOut("GET", name, null);
    }

    /** Overrides the level of one logger on every member of the cluster. */
    public JsonObject clusterSetLevel(String name, Level level, String client) {
        log.info(
                "Logger level change requested for the whole cluster: logger={}, level={}, "
                        + "client={}",
                name,
                level,
                client);
        return fanOut("POST", name, level);
    }

    /** Reverts one logger to its pre-override level on every member of the cluster. */
    public JsonObject clusterResetLevel(String name, String client) {
        log.info(
                "Logger level revert requested for the whole cluster: logger={}, client={}",
                name,
                client);
        return fanOut("DELETE", name, null);
    }

    /**
     * Runs the node local endpoint of every member and collects the answers. Members are
     * independent of each other, so a member that cannot be reached does not undo the change on the
     * members that were already changed; the per member status makes that visible instead.
     */
    private JsonObject fanOut(String method, String name, Level level) {
        HttpConfig httpConfig = httpConfig();
        JsonArray nodes = new JsonArray();
        int reached = 0;
        int failed = 0;
        for (Member member : nodeEngine.getClusterService().getMembers()) {
            String memberId = member.getAddress().getHost();
            try {
                int httpPort =
                        (int)
                                NodeEngineUtil.sendOperationToMemberNode(
                                                nodeEngine,
                                                new GetNodeHttpPortOperation(),
                                                member.getAddress())
                                        .get();
                memberId = member.getAddress().getHost() + ":" + httpPort;
                String url =
                        "http://"
                                + member.getAddress().getHost()
                                + ":"
                                + httpPort
                                + httpConfig.getContextPath()
                                + RestConstant.REST_URL_LOGGERS
                                + (name == null ? "" : "/" + encode(name))
                                + (level == null ? "" : "?" + LEVEL + "=" + encode(level.name()));
                nodes.add(request(url, method, httpConfig));
                reached++;
            } catch (Throwable t) {
                log.warn("Logger level request to member {} failed", memberId, t);
                nodes.add(
                        new JsonObject()
                                .add(NODE, memberId)
                                .add(STATUS, FAILURE)
                                .add(ERROR, errorMessage(t)));
                failed++;
            }
        }
        String status = failed == 0 ? SUCCESS : (reached == 0 ? FAILURE : PARTIAL_FAILURE);
        JsonObject response = new JsonObject().add(SCOPE, SCOPE_CLUSTER).add(STATUS, status);
        if (name != null) {
            response.add(NAME, name);
        }
        if (level != null) {
            response.add(LEVEL, level.name());
        }
        return response.add(NODES, nodes);
    }

    private JsonObject request(String url, String method, HttpConfig httpConfig)
            throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        try {
            connection.setRequestMethod(method);
            connection.setConnectTimeout(REQUEST_TIMEOUT_MS);
            connection.setReadTimeout(REQUEST_TIMEOUT_MS);
            if (httpConfig.isEnableBasicAuth()) {
                String credentials =
                        httpConfig.getBasicAuthUsername() + ":" + httpConfig.getBasicAuthPassword();
                connection.setRequestProperty(
                        "Authorization",
                        "Basic "
                                + Base64.getEncoder()
                                        .encodeToString(
                                                credentials.getBytes(StandardCharsets.UTF_8)));
            }
            connection.connect();
            int code = connection.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                throw new IOException(
                        "HTTP " + code + " from " + url + ": " + read(connection.getErrorStream()));
            }
            return Json.parse(read(connection.getInputStream())).asObject();
        } finally {
            connection.disconnect();
        }
    }

    private JsonObject loggerJson(String name, Level level) {
        JsonObject logger =
                new JsonObject()
                        .add(NAME, name)
                        .add(LEVEL, level == null ? null : level.name())
                        .add(ORIGIN, LogLevels.origin(name));
        Level fileLevel = LogLevels.levelBeforeOverride(name);
        if (fileLevel != null) {
            logger.add(FILE_LEVEL, fileLevel.name());
        }
        return logger;
    }

    private HttpConfig httpConfig() {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        if (seaTunnelServer == null) {
            throw new IllegalStateException("SeaTunnel server is not available on this node.");
        }
        return seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
    }

    private String nodeId() {
        return nodeEngine.getThisAddress().getHost() + ":" + httpConfig().getPort();
    }

    /**
     * Short reason of a failed member request, the stack trace goes to the node log instead. The
     * root cause is reported, because the wrappers a failed operation collects on the way out
     * ({@code CompletionException} around an {@code ExecutionException} around the real failure)
     * say nothing about what went wrong.
     */
    private static String errorMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null
                ? cause.getClass().getName()
                : cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not encode '" + value + "'", e);
        }
    }

    private static String read(InputStream stream) throws IOException {
        if (stream == null) {
            return "";
        }
        try (InputStream input = stream;
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int length;
            while ((length = input.read(buffer)) != -1) {
                output.write(buffer, 0, length);
            }
            return output.toString(StandardCharsets.UTF_8.name());
        }
    }
}

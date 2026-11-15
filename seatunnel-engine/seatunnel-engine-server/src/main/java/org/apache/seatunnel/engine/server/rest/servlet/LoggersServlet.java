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

import org.apache.seatunnel.engine.server.log.LogLevels;
import org.apache.seatunnel.engine.server.rest.service.LoggerLevelService;

import org.apache.logging.log4j.Level;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Reads and changes runtime log levels.
 *
 * <p>{@code GET /loggers} lists every configured logger, {@code GET /loggers/{name}} reads one,
 * {@code POST /loggers/{name}} overrides its level and {@code DELETE /loggers/{name}} reverts the
 * override. {@code ?scope=cluster} runs the same request on every member of the cluster.
 */
public class LoggersServlet extends BaseServlet {

    private final LoggerLevelService loggerLevelService;

    public LoggersServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.loggerLevelService = new LoggerLevelService(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String name = loggerName(req);
        if (!isScopeValid(req, resp)) {
            return;
        }
        if (isClusterRequested(req)) {
            writeJson(resp, loggerLevelService.clusterLoggers(name));
        } else if (name == null) {
            writeJson(resp, loggerLevelService.loggers());
        } else {
            writeJson(resp, loggerLevelService.logger(name));
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String name = loggerName(req);
        if (name == null) {
            badRequest(resp, "The logger name must not be empty, use /loggers/{name}.");
            return;
        }
        if (!isScopeValid(req, resp)) {
            return;
        }
        String levelName = requestedLevel(req);
        if (levelName == null) {
            badRequest(
                    resp,
                    "The level is required, send it as ?level=DEBUG or as {\"level\":\"DEBUG\"}.");
            return;
        }
        Level level = LogLevels.parse(levelName);
        if (level == null) {
            badRequest(
                    resp,
                    "Unknown logger level '"
                            + levelName
                            + "', valid levels are: "
                            + LogLevels.validNames());
            return;
        }
        String client = req.getRemoteAddr();
        if (isClusterRequested(req)) {
            writeJson(resp, loggerLevelService.clusterSetLevel(name, level, client));
        } else {
            writeJson(resp, loggerLevelService.setLevel(name, level, client));
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String name = loggerName(req);
        if (name == null) {
            badRequest(resp, "The logger name must not be empty, use /loggers/{name}.");
            return;
        }
        if (!isScopeValid(req, resp)) {
            return;
        }
        String client = req.getRemoteAddr();
        if (isClusterRequested(req)) {
            writeJson(resp, loggerLevelService.clusterResetLevel(name, client));
        } else {
            writeJson(resp, loggerLevelService.resetLevel(name, client));
        }
    }

    /** The logger name of the request path, {@code null} when the whole list is addressed. */
    private String loggerName(HttpServletRequest req) {
        String pathInfo = req.getPathInfo();
        if (pathInfo == null) {
            return null;
        }
        String name = pathInfo.startsWith("/") ? pathInfo.substring(1) : pathInfo;
        name = name.trim();
        return name.isEmpty() ? null : name;
    }

    /**
     * The level of the {@code level} query parameter, falling back to the {@code level} field of
     * the request body, or {@code null} when neither carries one.
     */
    private String requestedLevel(HttpServletRequest req) throws IOException {
        String level = req.getParameter(LoggerLevelService.LEVEL);
        if (level != null) {
            return level;
        }
        byte[] body = requestBody(req);
        if (body.length == 0) {
            return null;
        }
        try {
            JsonValue json = Json.parse(new String(body, StandardCharsets.UTF_8));
            return json.isObject()
                    ? json.asObject().getString(LoggerLevelService.LEVEL, null)
                    : null;
        } catch (RuntimeException e) {
            // an unparsable body is reported as a missing level instead of a server error
            return null;
        }
    }

    private boolean isClusterRequested(HttpServletRequest req) {
        return LoggerLevelService.SCOPE_CLUSTER.equalsIgnoreCase(
                req.getParameter(LoggerLevelService.SCOPE));
    }

    /** Rejects an unknown scope and returns whether the request may go on. */
    private boolean isScopeValid(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        String scope = req.getParameter(LoggerLevelService.SCOPE);
        if (scope == null
                || LoggerLevelService.SCOPE_CLUSTER.equalsIgnoreCase(scope)
                || LoggerLevelService.SCOPE_NODE.equalsIgnoreCase(scope)) {
            return true;
        }
        badRequest(
                resp,
                "Unknown scope '"
                        + scope
                        + "', valid scopes are: "
                        + LoggerLevelService.SCOPE_NODE
                        + ", "
                        + LoggerLevelService.SCOPE_CLUSTER);
        return false;
    }

    private void badRequest(HttpServletResponse resp, String message) throws IOException {
        writeJson(
                resp,
                new JsonObject().add("status", "fail").add("message", message),
                HttpServletResponse.SC_BAD_REQUEST);
    }
}

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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.google.gson.Gson;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class BaseServlet extends HttpServlet {

    protected final NodeEngineImpl nodeEngine;

    public BaseServlet(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    protected void writeJson(HttpServletResponse resp, Object obj) throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json; charset=UTF-8");
        resp.getWriter().write(new Gson().toJson(obj));
    }

    protected void writeJson(HttpServletResponse resp, JsonArray jsonArray) throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json; charset=UTF-8");
        resp.getWriter().write(jsonArray.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonObject jsonObject) throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json; charset=UTF-8");
        resp.getWriter().write(jsonObject.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonArray jsonArray, int statusCode)
            throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json; charset=UTF-8");
        resp.setStatus(statusCode);
        resp.getWriter().write(jsonArray.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonObject jsonObject, int statusCode)
            throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json; charset=UTF-8");
        resp.setStatus(statusCode);
        resp.getWriter().write(jsonObject.toString());
    }

    protected void writeJson(HttpServletResponse resp, Object obj, int statusCode)
            throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json; charset=UTF-8");
        resp.setStatus(statusCode);
        resp.getWriter().write(new Gson().toJson(obj));
    }

    protected void write(HttpServletResponse resp, Object obj) throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("text/plain; charset=UTF-8");
        resp.getWriter().write(obj.toString());
    }

    protected void writeHtml(HttpServletResponse resp, Object obj) throws IOException {
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("text/html; charset=UTF-8");
        resp.getWriter().write(obj.toString());
    }

    protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
        Map<String, Object> extensionServices =
                nodeEngine.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        if (shouldBeMaster && !seaTunnelServer.isMasterNode()) {
            return null;
        }
        return seaTunnelServer;
    }

    protected byte[] requestBody(HttpServletRequest req) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        try (BufferedReader reader = req.getReader()) {
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
        }

        String requestBody = stringBuilder.toString();
        return requestBody.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] requestHoconBody(HttpServletRequest req) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        try (BufferedReader reader = req.getReader()) {
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
            }
        }

        String requestBody = stringBuilder.toString();
        return requestBody.getBytes(StandardCharsets.UTF_8);
    }

    protected Map<String, String> getParameterMap(HttpServletRequest req) {
        Map<String, String> reqParameterMap = new HashMap<>();

        Map<String, String[]> parameterMap = req.getParameterMap();

        for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
            String paramName = entry.getKey();
            String[] paramValues = entry.getValue();

            for (String value : paramValues) {
                reqParameterMap.put(paramName, value);
            }
        }
        return reqParameterMap;
    }
}

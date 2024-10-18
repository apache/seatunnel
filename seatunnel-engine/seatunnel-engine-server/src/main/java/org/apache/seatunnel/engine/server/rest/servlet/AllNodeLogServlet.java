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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.log.FormatType;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_LOGS;

@Slf4j
public class AllNodeLogServlet extends LogBaseServlet {

    public AllNodeLogServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();
        int port = httpConfig.getPort();
        String uri = req.getRequestURI();

        // Analysis uri, get logName and jobId param
        String param = getLogParam(uri, contextPath);
        boolean isLogFile = param.contains(".log");
        String logName = isLogFile ? param : StringUtils.EMPTY;
        String jobId = !isLogFile ? param : StringUtils.EMPTY;

        String logPath = getLogPath();
        JsonArray systemMonitoringInformationJsonValues =
                getSystemMonitoringInformationJsonValues();

        if (StringUtils.isBlank(logName)) {
            StringBuffer logLink = new StringBuffer();
            ArrayList<Tuple3<String, String, String>> allLogNameList = new ArrayList<>();

            systemMonitoringInformationJsonValues.forEach(
                    systemMonitoringInformation -> {
                        String host = systemMonitoringInformation.asObject().get("host").asString();
                        String url = "http://" + host + ":" + port + contextPath;
                        String allName = sendGet(url + GET_ALL_LOG_NAME);
                        log.debug(String.format("Request: %s , Result: %s", url, allName));
                        ArrayNode jsonNodes = JsonUtils.parseArray(allName);

                        jsonNodes.forEach(
                                jsonNode -> {
                                    String fileName = jsonNode.asText();
                                    if (StringUtils.isNotBlank(jobId)
                                            && !fileName.contains(jobId)) {
                                        return;
                                    }
                                    allLogNameList.add(
                                            Tuple3.tuple3(
                                                    host + ":" + port,
                                                    url + GET_LOGS + "/" + fileName,
                                                    fileName));
                                });
                    });

            FormatType formatType = FormatType.fromString(req.getParameter("format"));
            switch (formatType) {
                case JSON:
                    JsonArray jsonArray =
                            allLogNameList.stream()
                                    .map(
                                            tuple -> {
                                                JsonObject jsonObject = new JsonObject();
                                                jsonObject.add("node", tuple.f0());
                                                jsonObject.add("logLink", tuple.f1());
                                                jsonObject.add("logName", tuple.f2());
                                                return jsonObject;
                                            })
                                    .collect(JsonArray::new, JsonArray::add, JsonArray::add);
                    writeJson(resp, jsonArray);
                    return;
                case HTML:
                default:
                    allLogNameList.forEach(
                            tuple ->
                                    logLink.append(
                                            buildLogLink(
                                                    tuple.f1(), tuple.f0() + "-" + tuple.f2())));
                    String logContent = buildWebSiteContent(logLink);
                    writeHtml(resp, logContent);
            }
        } else {
            prepareLogResponse(resp, logPath, logName);
        }
    }
}

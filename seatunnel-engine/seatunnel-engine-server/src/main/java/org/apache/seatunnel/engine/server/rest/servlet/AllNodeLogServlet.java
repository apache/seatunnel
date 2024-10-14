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

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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
        String logName = isLogFile ? param : "";
        String jobId = !isLogFile ? param : "";

        String logPath = getLogPath();
        JsonArray systemMonitoringInformationJsonValues =
                getSystemMonitoringInformationJsonValues();

        if (StringUtils.isBlank(logName)) {
            StringBuffer logLink = new StringBuffer();
            ArrayList<Tuple2<String, String>> allLogNameList = new ArrayList<>();

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
                                            Tuple2.tuple2(
                                                    host + ":" + port + "-" + fileName,
                                                    url + GET_LOGS + "/" + fileName));
                                });
                    });

            allLogNameList.forEach(
                    tuple2 -> logLink.append(buildLogLink(tuple2.f1(), tuple2.f0())));
            String logContent = buildWebSiteContent(logLink);
            writeHtml(resp, logContent);
        } else {
            prepareLogResponse(resp, logPath, logName);
        }
    }

    private String sendGet(String urlString) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();

            if (connection.getResponseCode() == 200) {
                try (InputStream is = connection.getInputStream();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = is.read(buffer)) != -1) {
                        baos.write(buffer, 0, len);
                    }
                    return baos.toString();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

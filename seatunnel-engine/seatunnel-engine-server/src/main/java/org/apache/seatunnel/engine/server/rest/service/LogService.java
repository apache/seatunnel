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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple3;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOGS;

@Slf4j
public class LogService extends BaseLogService {
    public LogService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public List<String> allLogName() {
        String logPath = getLogPath();
        List<File> logFileList = FileUtils.listFile(logPath);
        if (logFileList == null) {
            return new ArrayList<>();
        }
        return logFileList.stream().map(File::getName).collect(Collectors.toList());
    }

    public List<Tuple3<String, String, String>> allLogNameList(String jobId) {

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();
        int port = httpConfig.getPort();

        // Take BasicAuth from configuration (if enabled)
        HttpBasic result = getHttpBasicAuth(httpConfig);

        List<Tuple3<String, String, String>> allLogNameList = new ArrayList<>();

        JsonArray systemMonitoringInformationJsonValues =
                getSystemMonitoringInformationJsonValues();
        systemMonitoringInformationJsonValues.forEach(
                systemMonitoringInformation -> {
                    String host = systemMonitoringInformation.asObject().get("host").asString();
                    String url = "http://" + host + ":" + port + contextPath;
                    String logUrl = url + REST_URL_GET_ALL_LOG_NAME;

                    String allName =
                            httpConfig.isEnableBasicAuth()
                                    ? sendGet(logUrl, result.basicUser, result.basicPass)
                                    : sendGet(logUrl);

                    if (allName == null || allName.trim().isEmpty()) {
                        log.warn(
                                "GET {} returned empty body (null/empty). Skip this node.", logUrl);
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Request: {} , Result: {}", url, allName);
                    }
                    ArrayNode jsonNodes = JsonUtils.parseArray(allName);

                    jsonNodes.forEach(
                            jsonNode -> {
                                String fileName = jsonNode.asText();
                                if (StringUtils.isNotBlank(jobId) && !fileName.contains(jobId)) {
                                    return;
                                }
                                allLogNameList.add(
                                        new Tuple3<>(
                                                host + ":" + port,
                                                url + REST_URL_LOGS + "/" + fileName,
                                                fileName));
                            });
                });

        return allLogNameList;
    }

    private static HttpBasic getHttpBasicAuth(HttpConfig httpConfig) {
        String basicUser = "";
        String basicPass = "";
        try {
            if (httpConfig.isEnableBasicAuth()) {
                basicUser = httpConfig.getBasicAuthUsername();
                basicPass = httpConfig.getBasicAuthPassword();
            }
        } catch (Throwable ignore) {
            // Compatible with older versions: If HttpConfig does not have these methods, use system
            // properties or environment variables to find out
            basicUser = System.getProperty("seatunnel.http.user", System.getenv("BASIC_AUTH_USER"));
            basicPass = System.getProperty("seatunnel.http.pass", System.getenv("BASIC_AUTH_PASS"));
            log.warn("Use system property or environment variable to set basic auth.");
        }

        if (StringUtils.isNotBlank(basicUser) && StringUtils.isNotBlank(basicPass)) {
            httpConfig.setBasicAuthUsername(basicUser);
            httpConfig.setBasicAuthPassword(basicPass);
        }
        return new HttpBasic(basicUser, basicPass);
    }

    private static class HttpBasic {

        public final String basicUser;
        public final String basicPass;

        public HttpBasic(String basicUser, String basicPass) {
            this.basicUser = basicUser;
            this.basicPass = basicPass;
        }
    }

    public JsonArray allNodeLogFormatJson(String jobId) {

        return allLogNameList(jobId).stream()
                .map(
                        tuple -> {
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.add("node", tuple._1());
                            jsonObject.add("logLink", tuple._2());
                            jsonObject.add("logName", tuple._3());
                            return jsonObject;
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }

    public String allNodeLogFormatHtml(String jobId) {
        StringBuffer logLink = new StringBuffer();

        allLogNameList(jobId)
                .forEach(tuple -> logLink.append(buildLogLink(tuple._2(), tuple._3())));
        return buildWebSiteContent(logLink);
    }

    public String currentNodeLog() {
        List<File> logFileList = FileUtils.listFile(getLogPath());
        StringBuffer logLink = new StringBuffer();
        if (logFileList != null) {
            for (File file : logFileList) {
                logLink.append(buildLogLink("log/" + file.getName(), file.getName()));
            }
        }

        return buildWebSiteContent(logLink);
    }
}

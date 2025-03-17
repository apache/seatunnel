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

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import org.apache.commons.lang3.StringUtils;

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
            return null;
        }
        return logFileList.stream().map(File::getName).collect(Collectors.toList());
    }

    public List<Tuple3<String, String, String>> allLogNameList(String jobId) {

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();
        int port = httpConfig.getPort();

        List<Tuple3<String, String, String>> allLogNameList = new ArrayList<>();

        JsonArray systemMonitoringInformationJsonValues =
                getSystemMonitoringInformationJsonValues();
        systemMonitoringInformationJsonValues.forEach(
                systemMonitoringInformation -> {
                    String host = systemMonitoringInformation.asObject().get("host").asString();
                    String url = "http://" + host + ":" + port + contextPath;
                    String allName = sendGet(url + REST_URL_GET_ALL_LOG_NAME);
                    log.debug(String.format("Request: %s , Result: %s", url, allName));
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

    public String currentNodeLog(String uri) {
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

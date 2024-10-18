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

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Slf4j
public class CurrentNodeLogServlet extends LogBaseServlet {

    public CurrentNodeLogServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();
        String uri = req.getRequestURI();
        String logName = getLogParam(uri, contextPath);
        String logPath = getLogPath();

        if (StringUtils.isBlank(logName)) {
            // Get Current Node Log List
            List<File> logFileList = FileUtils.listFile(logPath);
            StringBuffer logLink = new StringBuffer();
            for (File file : logFileList) {
                logLink.append(buildLogLink("log/" + file.getName(), file.getName()));
            }
            writeHtml(resp, buildWebSiteContent(logLink));
        } else {
            // Get Current Node Log Content
            prepareLogResponse(resp, logPath, logName);
        }
    }
}

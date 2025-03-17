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

import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.log.FormatType;
import org.apache.seatunnel.engine.server.rest.service.LogService;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

@Slf4j
public class AllNodeLogServlet extends LogBaseServlet {

    private final LogService logService;

    public AllNodeLogServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.logService = new LogService(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();
        String uri = req.getRequestURI();

        // Analysis uri, get logName and jobId param
        String param = logService.getLogParam(uri, contextPath);
        boolean isLogFile = param.contains(".log");
        String logName = isLogFile ? param : StringUtils.EMPTY;
        String jobId = !isLogFile ? param : StringUtils.EMPTY;

        String logPath = logService.getLogPath();

        if (StringUtils.isBlank(logName)) {

            FormatType formatType = FormatType.fromString(req.getParameter("format"));
            switch (formatType) {
                case JSON:
                    writeJson(resp, logService.allNodeLogFormatJson(jobId));
                    return;
                case HTML:
                default:
                    writeHtml(resp, logService.allNodeLogFormatHtml(jobId));
            }
        } else {
            prepareLogResponse(resp, logPath, logName);
        }
    }
}

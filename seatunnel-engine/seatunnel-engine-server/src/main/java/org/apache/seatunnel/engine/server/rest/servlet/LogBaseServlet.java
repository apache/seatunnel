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

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.FileUtils;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;

@Slf4j
public class LogBaseServlet extends BaseServlet {

    public LogBaseServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }
    /** Prepare Log Response */
    protected void prepareLogResponse(HttpServletResponse resp, String logPath, String logName) {
        if (StringUtils.isBlank(logPath)) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            log.warn(
                    "Log file path is empty, no log file path configured in the current configuration file");
            return;
        }
        String logFilePath = logPath + "/" + logName;
        try {
            String logContent = FileUtils.readFileToStr(new File(logFilePath).toPath());
            write(resp, logContent);
        } catch (SeaTunnelRuntimeException | IOException e) {
            // If the log file does not exist, return 400
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            log.warn(String.format("Log file content is empty, get log path : %s", logFilePath));
        }
    }
}

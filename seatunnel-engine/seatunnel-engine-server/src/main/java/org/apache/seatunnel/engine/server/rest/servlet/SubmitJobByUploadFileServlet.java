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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;

import org.apache.seatunnel.engine.server.rest.service.JobInfoService;

import org.apache.commons.io.IOUtils;

import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SubmitJobByUploadFileServlet extends BaseServlet {
    private final JobInfoService jobInfoService;

    public SubmitJobByUploadFileServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.jobInfoService = new JobInfoService(nodeEngine);
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException, ServletException {

        Part filePart = req.getPart("config_file");
        String submittedFileName = filePart.getSubmittedFileName();
        String content = IOUtils.toString(filePart.getInputStream(), StandardCharsets.UTF_8);
        Config config;
        if (submittedFileName.endsWith(".json")) {
            config =
                    ConfigFactory.parseString(
                            content, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON));
        } else {
            config = ConfigFactory.parseString(content);
        }
        writeJson(resp, jobInfoService.submitJob(getParameterMap(req), config));
    }
}

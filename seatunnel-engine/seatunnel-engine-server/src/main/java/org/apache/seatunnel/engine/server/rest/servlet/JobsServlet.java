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

import org.apache.seatunnel.engine.server.rest.service.JobMonitoringService;

import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Provides a lightweight, incremental view of terminal job state changes for monitoring systems.
 */
public class JobsServlet extends BaseServlet {

    // Performs the bounded, sequence-based finished-state query.
    private final JobMonitoringService jobMonitoringService;

    /**
     * Creates the monitoring servlet for the local SeaTunnel node.
     *
     * @param nodeEngine local Hazelcast node engine
     */
    public JobsServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.jobMonitoringService = new JobMonitoringService(nodeEngine);
    }

    /**
     * Returns one bounded sequence window after the caller's start position or opaque cursor.
     *
     * @param req HTTP request containing the monitoring query parameters
     * @param resp HTTP response
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        writeJson(
                resp,
                jobMonitoringService.getFinishedJobChanges(
                        req.getParameter("status"),
                        req.getParameter("start"),
                        req.getParameter("cursor"),
                        req.getParameter("limit")));
    }
}

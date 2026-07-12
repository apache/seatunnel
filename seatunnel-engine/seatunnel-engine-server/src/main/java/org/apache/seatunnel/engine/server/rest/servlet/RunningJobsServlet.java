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

import org.apache.seatunnel.shade.org.eclipse.jetty.server.Request;

import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.rest.service.JobInfoService;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RunningJobsServlet extends PageBaseServlet {

    private final JobInfoService jobInfoService;

    public RunningJobsServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.jobInfoService = new JobInfoService(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        long nowMs = System.currentTimeMillis();
        long receivedMs = -1;
        try {
            Request baseRequest = Request.getBaseRequest(req);
            if (baseRequest != null) {
                receivedMs = baseRequest.getTimeStamp();
            }
        } catch (Throwable ignored) {
            // ignore
        }

        long startNs = System.nanoTime();
        boolean summary =
                req.getRequestURI() != null
                        && req.getRequestURI().endsWith(RestConstant.REST_URL_RUNNING_JOBS_SUMMARY);
        boolean full = !summary || Boolean.parseBoolean(req.getParameter("full"));

        JsonArray runningJobs =
                full
                        ? jobInfoService.getRunningJobsJson()
                        : jobInfoService.getRunningJobsSummaryJson();

        long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        long dispatchDelayMs = receivedMs <= 0 ? -1 : Math.max(0, nowMs - receivedMs);

        resp.setHeader("X-Dispatch-Delay-Ms", String.valueOf(dispatchDelayMs));
        resp.setHeader("X-Handler-Cost-Ms", String.valueOf(costMs));

        writeJsonWithPagination(req, resp, runningJobs);

        if (dispatchDelayMs > 500) {
            log.warn(
                    "GET /running-jobs dispatch delayed: dispatchDelayMs={} thread={}",
                    dispatchDelayMs,
                    Thread.currentThread().getName());
        }
        if (costMs > 500) {
            log.warn("GET /running-jobs slow: full={} costMs={}", full, costMs);
            Runtime rt = Runtime.getRuntime();
            long usedBytes = rt.totalMemory() - rt.freeMemory();
            log.warn(
                    "GET /running-jobs slow diagnostics: full={} costMs={} thread={} "
                            + "heapUsedMB={} heapTotalMB={} heapMaxMB={}",
                    full,
                    costMs,
                    Thread.currentThread().getName(),
                    usedBytes / 1024 / 1024,
                    rt.totalMemory() / 1024 / 1024,
                    rt.maxMemory() / 1024 / 1024);
        } else {
            log.debug("GET /running-jobs: full={} costMs={}", full, costMs);
        }
    }
}

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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.server.rest.service.OverviewService;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OverviewServlet extends BaseServlet {

    private final OverviewService overviewService;

    public OverviewServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.overviewService = new OverviewService(nodeEngine);
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
        Map<String, String> tags = getParameterMap(req);

        JsonObject body =
                JsonUtil.toJsonObject(
                        JsonUtils.toMap(
                                JsonUtils.toJsonString(overviewService.getOverviewInfo(tags))));

        long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        long dispatchDelayMs = receivedMs <= 0 ? -1 : Math.max(0, nowMs - receivedMs);

        resp.setHeader("X-Dispatch-Delay-Ms", String.valueOf(dispatchDelayMs));
        resp.setHeader("X-Handler-Cost-Ms", String.valueOf(costMs));

        writeJson(resp, body);
        if (dispatchDelayMs > 500) {
            log.warn(
                    "GET /overview dispatch delayed: dispatchDelayMs={} thread={}",
                    dispatchDelayMs,
                    Thread.currentThread().getName());
        }
        if (costMs > 500) {
            Runtime rt = Runtime.getRuntime();
            long usedBytes = rt.totalMemory() - rt.freeMemory();
            log.warn(
                    "GET /overview slow: costMs={} thread={} heapUsedMB={} heapTotalMB={} "
                            + "heapMaxMB={}",
                    costMs,
                    Thread.currentThread().getName(),
                    usedBytes / 1024 / 1024,
                    rt.totalMemory() / 1024 / 1024,
                    rt.maxMemory() / 1024 / 1024);
        }
    }
}

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

import org.apache.seatunnel.engine.server.NodeExtension;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import com.hazelcast.spi.impl.NodeEngineImpl;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.StringWriter;

public class MetricsServlet extends BaseServlet {

    private final CollectorRegistry collectorRegistry;

    public MetricsServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        NodeExtension nodeExtension = (NodeExtension) nodeEngine.getNode().getNodeExtension();
        collectorRegistry = nodeExtension.getCollectorRegistry();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String servletPath = req.getServletPath();
        String contentType;
        if (servletPath.endsWith(RestConstant.REST_URL_METRICS)) {
            contentType = TextFormat.CONTENT_TYPE_004;
        } else if (servletPath.endsWith(RestConstant.REST_URL_OPEN_METRICS)) {
            contentType = TextFormat.CONTENT_TYPE_OPENMETRICS_100;
        } else {
            // should not happen, because the servlet is only registered for /metrics and
            // /open-metrics
            throw new IllegalArgumentException("Unsupported metrics format");
        }
        try (StringWriter stringWriter = new StringWriter()) {
            TextFormat.writeFormat(
                    contentType, stringWriter, collectorRegistry.metricFamilySamples());
            write(resp, stringWriter.toString());
        }
    }
}

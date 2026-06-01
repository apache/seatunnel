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

import org.apache.seatunnel.api.metadata.MetadataConfig;
import org.apache.seatunnel.engine.server.rest.service.DynamicMetadataDataSourceService;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

public class DynamicMetadataDataSourceServlet extends BaseServlet {

    private final DynamicMetadataDataSourceService metadataDataSourceService;

    public DynamicMetadataDataSourceServlet(
            NodeEngineImpl nodeEngine, MetadataConfig metadataConfig) {
        super(nodeEngine);
        this.metadataDataSourceService =
                new DynamicMetadataDataSourceService(nodeEngine, metadataConfig);
    }

    /**
     * Handle GET requests: - GET /metadata/datasource/{datasourceId} - Get a specific datasource -
     * GET /metadata/datasources - List all datasources
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String pathInfo = req.getPathInfo();

        // GET /metadata/datasources - List all datasources
        if (pathInfo == null || pathInfo.equals("/")) {
            // Check if this is the list endpoint
            String servletPath = req.getServletPath();
            if (servletPath != null && servletPath.endsWith("/datasources")) {
                writeJson(resp, metadataDataSourceService.listDatasources());
                return;
            }
            // Default to list if no datasourceId provided
            writeJson(resp, metadataDataSourceService.listDatasources());
            return;
        }

        // GET /metadata/datasource/{datasourceId} - Get specific datasource
        String datasourceId = pathInfo.startsWith("/") ? pathInfo.substring(1) : pathInfo;
        JsonObject result = metadataDataSourceService.getDatasource(datasourceId);
        writeJson(resp, result, getStatusCode(result));
    }

    /** Handle POST requests: - POST /metadata/datasource - Create a new datasource */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        JsonObject result = metadataDataSourceService.createDatasource(requestBody(req));
        writeJson(resp, result, getStatusCode(result));
    }

    /** Handle PUT requests: - PUT /metadata/datasource/{datasourceId} - Update a datasource */
    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String pathInfo = req.getPathInfo();
        if (pathInfo == null || pathInfo.equals("/")) {
            writeJson(
                    resp,
                    errorResponse("datasourceId is required in the URL path"),
                    HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String datasourceId = pathInfo.startsWith("/") ? pathInfo.substring(1) : pathInfo;
        JsonObject result =
                metadataDataSourceService.updateDatasource(datasourceId, requestBody(req));
        writeJson(resp, result, getStatusCode(result));
    }

    /**
     * Handle DELETE requests: - DELETE /metadata/datasource/{datasourceId} - Delete a datasource
     */
    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String pathInfo = req.getPathInfo();
        if (pathInfo == null || pathInfo.equals("/")) {
            writeJson(
                    resp,
                    errorResponse("datasourceId is required in the URL path"),
                    HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String datasourceId = pathInfo.startsWith("/") ? pathInfo.substring(1) : pathInfo;
        JsonObject result = metadataDataSourceService.deleteDatasource(datasourceId);
        writeJson(resp, result, getStatusCode(result));
    }

    /**
     * Extract HTTP status code from result JsonObject.
     *
     * @param result the result JsonObject
     * @return the HTTP status code
     */
    private int getStatusCode(JsonObject result) {
        String status = result.getString("status", "");
        if ("error".equals(status)) {
            return HttpServletResponse.SC_BAD_REQUEST;
        }
        return HttpServletResponse.SC_OK;
    }

    /**
     * Create an error response JsonObject.
     *
     * @param message the error message
     * @return the error JsonObject
     */
    private JsonObject errorResponse(String message) {
        return new JsonObject().add("status", "error").add("message", message);
    }
}

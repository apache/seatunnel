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

import org.apache.seatunnel.engine.server.rest.ErrResponse;
import org.apache.seatunnel.engine.server.rest.service.OptionRulesService;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

@Slf4j
public class OptionRulesServlet extends BaseServlet {

    private final OptionRulesService optionRulesService;

    public OptionRulesServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.optionRulesService = new OptionRulesService(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        Map<String, String> params = getParameterMap(req);
        String pluginType = params.get("type");
        String pluginName = params.get("plugin");
        try {
            writeJson(resp, optionRulesService.getOptionRules(pluginType, pluginName));
        } catch (IllegalArgumentException e) {
            log.warn(
                    "Invalid option rules request, type: {}, plugin: {}, error: {}",
                    pluginType,
                    pluginName,
                    e.getMessage());
            writeJson(resp, errorResponse(e.getMessage()), HttpServletResponse.SC_BAD_REQUEST);
        } catch (NoSuchElementException e) {
            log.info("Option rules plugin not found, type: {}, plugin: {}", pluginType, pluginName);
            writeJson(resp, errorResponse(e.getMessage()), HttpServletResponse.SC_NOT_FOUND);
        } catch (RuntimeException e) {
            log.error(
                    "Failed to load option rules, type: {}, plugin: {}", pluginType, pluginName, e);
            writeJson(
                    resp,
                    errorResponse("Failed to load option rules."),
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    private ErrResponse errorResponse(String message) {
        ErrResponse errResponse = new ErrResponse();
        errResponse.setStatus("FAIL");
        errResponse.setMessage(message);
        return errResponse;
    }
}

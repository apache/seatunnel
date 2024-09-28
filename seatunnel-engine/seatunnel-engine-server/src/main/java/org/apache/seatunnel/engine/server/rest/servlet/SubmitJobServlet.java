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

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;

import static org.apache.seatunnel.engine.server.rest.RestHttpPostCommandProcessor.requestHandle;
import static org.apache.seatunnel.engine.server.rest.RestHttpPostCommandProcessor.submitJobInternal;

public class SubmitJobServlet extends BaseServlet {
    public SubmitJobServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        Map<String, String> requestParams = getParameterMap(req);

        Config config = RestUtil.buildConfig(requestHandle(requestBody(req)), false);

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        JsonObject jsonObject =
                submitJobInternal(config, requestParams, seaTunnelServer, nodeEngine.getNode());
        writeJson(resp, jsonObject);
    }
}

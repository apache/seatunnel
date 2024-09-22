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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.JsonObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;

import static org.apache.seatunnel.engine.server.rest.RestHttpPostCommandProcessor.handleStopJob;
import static org.apache.seatunnel.engine.server.rest.RestHttpPostCommandProcessor.requestHandle;

public class StopJobServlet extends BaseServlet {
    public StopJobServlet(HazelcastInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        Map<String, Object> map = JsonUtils.toMap(requestHandle(requestBody(req)));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        Node node = hazelcastInstance.node;
        handleStopJob(map, seaTunnelServer, node);
        writeJson(
                resp,
                new JsonObject().add(RestConstant.JOB_ID, map.get(RestConstant.JOB_ID).toString()));
    }
}

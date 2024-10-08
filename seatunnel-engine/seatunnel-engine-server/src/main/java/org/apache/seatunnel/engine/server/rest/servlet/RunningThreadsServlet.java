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

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Comparator;

public class RunningThreadsServlet extends BaseServlet {

    public RunningThreadsServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        JsonArray runningThreads =
                Thread.getAllStackTraces().keySet().stream()
                        .sorted(Comparator.comparing(Thread::getName))
                        .map(
                                stackTraceElements -> {
                                    JsonObject jobInfoJson = new JsonObject();
                                    jobInfoJson.add("threadName", stackTraceElements.getName());
                                    jobInfoJson.add(
                                            "classLoader",
                                            String.valueOf(
                                                    stackTraceElements.getContextClassLoader()));
                                    return jobInfoJson;
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);

        writeJson(resp, runningThreads);
    }
}

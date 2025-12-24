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

package org.apache.seatunnel.engine.server.rest.service;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Comparator;

public class RunningThreadService extends BaseService {
    public RunningThreadService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonArray getRunningThread() {
        return Thread.getAllStackTraces().keySet().stream()
                .sorted(Comparator.comparing(Thread::getName))
                .map(
                        stackTraceElements -> {
                            JsonObject jobInfoJson = new JsonObject();
                            jobInfoJson.add("threadName", stackTraceElements.getName());
                            jobInfoJson.add(
                                    "classLoader",
                                    String.valueOf(stackTraceElements.getContextClassLoader()));
                            return jobInfoJson;
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }
}

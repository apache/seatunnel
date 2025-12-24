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

import java.util.Map;

public class ThreadDumpService extends BaseService {
    public ThreadDumpService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonArray getThreadDump() {

        Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
        JsonArray threadInfoList = new JsonArray();
        for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
            StringBuilder stackTraceBuilder = new StringBuilder();
            for (StackTraceElement element : entry.getValue()) {
                stackTraceBuilder.append(element.toString()).append("\n");
            }
            String stackTrace = stackTraceBuilder.toString().trim();
            JsonObject threadInfo = new JsonObject();
            threadInfo.add("threadName", entry.getKey().getName());
            threadInfo.add("threadId", entry.getKey().getId());
            threadInfo.add("threadState", entry.getKey().getState().name());
            threadInfo.add("stackTrace", stackTrace);
            threadInfoList.add(threadInfo);
        }

        return threadInfoList;
    }
}

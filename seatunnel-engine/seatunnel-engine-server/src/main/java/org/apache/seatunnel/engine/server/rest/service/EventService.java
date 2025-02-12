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

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.engine.common.Constant;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.ArrayBlockingQueue;

public class EventService extends BaseService {

    public EventService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonArray getEventInfoJson(Long jobId) {
        ArrayBlockingQueue<Event> events =
                (ArrayBlockingQueue<Event>)
                        nodeEngine
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_FINISHED_JOB_EVENT)
                                .get(jobId);
        JsonArray eventArray = new JsonArray();
        if (events == null) {
            return eventArray;
        }
        for (Event event : events) {
            JsonObject members = new JsonObject();
            members.add("createdTime", event.getCreatedTime());
            members.add("eventType", event.getEventType().toString());
            eventArray.add(members);
        }
        return eventArray;
    }
}

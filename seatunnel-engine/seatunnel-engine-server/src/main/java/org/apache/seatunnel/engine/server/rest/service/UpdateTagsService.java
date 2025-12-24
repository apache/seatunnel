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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.stream.Collectors;

public class UpdateTagsService extends BaseService {
    public UpdateTagsService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonObject updateTags(byte[] requestBody) {
        Map<String, Object> params = JsonUtils.toMap(requestHandle(requestBody));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);

        NodeEngineImpl nodeEngine = seaTunnelServer.getNodeEngine();
        MemberImpl localMember = nodeEngine.getLocalMember();

        Map<String, String> tags =
                params.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        value ->
                                                value.getValue() != null
                                                        ? value.getValue().toString()
                                                        : ""));
        localMember.updateAttribute(tags);
        return new JsonObject().add("status", "success").add("message", "update node tags done.");
    }
}

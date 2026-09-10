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

    /**
     * Updates the local member tags from the request body.
     *
     * @param requestBody JSON request body containing the legacy flat tag map.
     * @return operation status JSON.
     */
    public JsonObject updateTags(byte[] requestBody) {
        return updateTags(JsonUtils.toMap(requestHandle(requestBody)), getLocalMember());
    }

    /**
     * Updates tags through the structured, target-validated Web UI request format.
     *
     * @param requestBody JSON request body containing {@code uuid} and a nested {@code tags}
     *     object.
     * @return operation status JSON.
     */
    public JsonObject updateLocalMemberTags(byte[] requestBody) {
        Map<String, Object> params = JsonUtils.toMap(requestHandle(requestBody));
        MemberImpl localMember = getLocalMember();
        validateTargetMember(params, localMember);
        return updateTags(extractStructuredTagParams(params), localMember);
    }

    private MemberImpl getLocalMember() {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        NodeEngineImpl nodeEngine = seaTunnelServer.getNodeEngine();
        return nodeEngine.getLocalMember();
    }

    private JsonObject updateTags(Map<String, Object> tagParams, MemberImpl localMember) {
        Map<String, String> tags = toStringTags(tagParams);
        localMember.updateAttribute(tags);
        return new JsonObject().add("status", "success").add("message", "update node tags done.");
    }

    /**
     * Validates that the target uuid matches the local member.
     *
     * @param params parsed request parameters.
     * @param localMember member served by the current REST endpoint.
     */
    private void validateTargetMember(Map<String, Object> params, MemberImpl localMember) {
        Object uuid = params.get("uuid");
        if (uuid == null || !localMember.getUuid().toString().equals(uuid.toString())) {
            throw new IllegalArgumentException(
                    "Target member uuid must match the REST node serving this request.");
        }
    }

    /**
     * Extracts tags from the structured request format.
     *
     * @param params parsed request parameters.
     * @return tag map from the structured request.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> extractStructuredTagParams(Map<String, Object> params) {
        Object tags = params.get("tags");
        if (!(tags instanceof Map)) {
            throw new IllegalArgumentException("The tags field must be an object.");
        }
        return (Map<String, Object>) tags;
    }

    /**
     * Converts JSON values to Hazelcast member attribute strings.
     *
     * @param tagParams raw tag values from the request.
     * @return string tags accepted by Hazelcast member attributes.
     */
    static Map<String, String> toStringTags(Map<String, Object> tagParams) {
        return tagParams.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                value ->
                                        value.getValue() != null
                                                ? value.getValue().toString()
                                                : ""));
    }
}

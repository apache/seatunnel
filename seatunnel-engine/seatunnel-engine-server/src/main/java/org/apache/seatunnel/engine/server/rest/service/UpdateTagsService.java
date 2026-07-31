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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Updates tags for the REST node that serves the request.
 *
 * <p>The optional member uuid prevents operators from accidentally updating the current node while
 * intending to update a different worker.
 */
public class UpdateTagsService extends BaseService {
    public UpdateTagsService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    /**
     * Updates the local member tags from the request body.
     *
     * @param requestBody JSON request body. It can be either a legacy flat tag map or a structured
     *     request with uuid and tags.
     * @return operation status JSON.
     */
    public JsonObject updateTags(byte[] requestBody) {
        Map<String, Object> params = JsonUtils.toMap(requestHandle(requestBody));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);

        NodeEngineImpl nodeEngine = seaTunnelServer.getNodeEngine();
        MemberImpl localMember = nodeEngine.getLocalMember();
        validateTargetMember(params, localMember);

        Map<String, Object> tagParams = extractTagParams(params);
        Map<String, String> tags = toStringTags(tagParams);
        localMember.updateAttribute(tags);
        return new JsonObject().add("status", "success").add("message", "update node tags done.");
    }

    /**
     * Validates that an explicit target uuid matches the local member.
     *
     * @param params parsed request parameters.
     * @param localMember member served by the current REST endpoint.
     */
    private void validateTargetMember(Map<String, Object> params, MemberImpl localMember) {
        Object uuid = params.get("uuid");
        if (uuid != null && !localMember.getUuid().toString().equals(uuid.toString())) {
            throw new IllegalArgumentException(
                    String.format(
                            "Target member uuid %s is not served by this REST node. "
                                    + "Please send the request to the target node.",
                            uuid));
        }
    }

    /**
     * Extracts tag values from the structured or legacy request format.
     *
     * @param params parsed request parameters.
     * @return tag map to apply to the local member.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractTagParams(Map<String, Object> params) {
        Object tags = params.get("tags");
        if (tags instanceof Map) {
            return (Map<String, Object>) tags;
        }
        if (tags != null) {
            throw new IllegalArgumentException("The tags field must be an object.");
        }
        Map<String, Object> legacyTags = new HashMap<>(params);
        legacyTags.remove("uuid");
        return legacyTags;
    }

    /**
     * Converts JSON values to Hazelcast member attribute strings.
     *
     * @param tagParams raw tag values from the request.
     * @return string tags accepted by Hazelcast member attributes.
     */
    private Map<String, String> toStringTags(Map<String, Object> tagParams) {
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.connector.file.output;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.edge.agent.connector.file.multiline.MultilineAssembler;
import org.apache.seatunnel.edge.agent.connector.record.CollectedRecord;

import java.util.List;

public class LineOutputFormatter implements OutputFormatter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CollectedRecord format(List<MultilineAssembler.LineElement> event, String sourceId) {
        if (event.isEmpty()) {
            throw new IllegalArgumentException("event must not be empty");
        }
        MultilineAssembler.LineElement first = event.get(0);
        MultilineAssembler.LineElement last = event.get(event.size() - 1);
        try {
            String json;
            if (event.size() == 1) {
                ObjectNode node = buildLineObject(0, first, first.getText());
                json = objectMapper.writeValueAsString(node);
            } else {
                ArrayNode array = objectMapper.createArrayNode();
                for (int i = 0; i < event.size(); i++) {
                    MultilineAssembler.LineElement e = event.get(i);
                    array.add(buildLineObject(i, e, e.getText()));
                }
                json = objectMapper.writeValueAsString(array);
            }
            return new CollectedRecord(
                    json,
                    sourceId,
                    first.getFilePath(),
                    last.getOffset(),
                    first.getLineNumber(),
                    first.getTs());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize event as JSON", e);
        }
    }

    private ObjectNode buildLineObject(
            int index, MultilineAssembler.LineElement line, String payloadText) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("_index", index);
        node.put("_file", line.getFilePath());
        node.put("_line", line.getLineNumber());
        node.put("_offset", line.getOffset());
        node.put("_ts", line.getTs());
        node.put("payload", payloadText);
        return node;
    }
}

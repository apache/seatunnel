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

package org.apache.seatunnel.connectors.seatunnel.splunk;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SplunkSourceReader extends HttpSourceReader {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public SplunkSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext readerContext,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentField) {
        super(httpParameter, readerContext, deserializationSchema, jsonField, contentField);
    }

    /**
     * The parent class's pollAndCollectData() already splits multi-line responses one line at a
     * time (when enableMultilines is true) and deserializes each line independently. Splunk's
     * export stream nests each row under "result" and interleaves "preview" rows that duplicate
     * later final rows, so we intercept here — before the parent's line splitting/deserialization
     * runs — to unwrap and de-duplicate.
     */
    @Override
    protected HttpResponse executeRequest() throws Exception {
        HttpResponse response = super.executeRequest();
        String filtered = filterAndUnwrapNdjson(response.getContent());
        return new HttpResponse(response.getCode(), filtered);
    }

    /**
     * Package-private and static so it can be unit tested directly, without mocking the HTTP
     * client. Drops Splunk "preview" rows and unwraps each remaining line's "result" object so the
     * parent reader's per-line deserialization sees plain row JSON.
     */
    static String filterAndUnwrapNdjson(String content) throws Exception {
        if (content == null || content.isEmpty()) {
            return content;
        }
        StringBuilder filtered = new StringBuilder();
        for (String line : content.split("\r?\n")) {
            if (line.trim().isEmpty()) {
                continue;
            }
            JsonNode jsonNode = OBJECT_MAPPER.readTree(line);

            JsonNode previewNode = jsonNode.get("preview");
            if (previewNode != null && previewNode.asBoolean(false)) {
                continue;
            }

            JsonNode resultNode = jsonNode.get("result");
            if (resultNode == null || resultNode.isNull()) {
                continue;
            }

            filtered.append(resultNode.toString()).append('\n');
        }
        return filtered.toString();
    }
}

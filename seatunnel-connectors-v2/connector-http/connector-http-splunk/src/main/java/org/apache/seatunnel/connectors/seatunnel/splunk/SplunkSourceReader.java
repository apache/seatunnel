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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSourceParameter;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@Slf4j
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

    @Override
    protected HttpResponse executeRequest() throws Exception {
        HttpResponse response = super.executeRequest();
        String content = response.getContent();

        if (content != null) {
            long maxBytes = ((SplunkSourceParameter) httpParameter).getMaxResponseSizeBytes();
            long contentSizeBytes = content.getBytes(StandardCharsets.UTF_8).length;

            if (contentSizeBytes > maxBytes) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.REQUEST_FAILED,
                        String.format(
                                "Splunk export response size (%d bytes) exceeds the configured "
                                        + "max_response_size_bytes limit (%d bytes). Narrow the "
                                        + "search's time window or result count (e.g. Splunk's "
                                        + "earliest/latest parameters or a smaller 'head' limit), "
                                        + "or raise max_response_size_bytes if you have confirmed "
                                        + "sufficient worker heap for the larger export.",
                                contentSizeBytes, maxBytes));
            }

            if (contentSizeBytes > maxBytes / 2) {
                log.warn(
                        "Splunk export response size ({} bytes) is more than half of the "
                                + "configured max_response_size_bytes limit ({} bytes); consider "
                                + "narrowing the search or reviewing worker heap sizing.",
                        contentSizeBytes,
                        maxBytes);
            }
        }

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

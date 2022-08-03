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

package org.apache.seatunnel.connectors.seatunnel.http.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

public class HttpSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkWriter.class);
    private final HttpClientProvider httpClient = HttpClientProvider.getInstance();
    private final SeaTunnelRowType seaTunnelRowType;
    private final HttpParameter httpParameter;

    public HttpSinkWriter(SeaTunnelRowType seaTunnelRowType, HttpParameter httpParameter) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.httpParameter = httpParameter;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<Object, Object> objectMap = new HashMap<>();
        int totalFields = seaTunnelRowType.getTotalFields();
        for (int i = 0; i < totalFields; i++) {
            objectMap.put(seaTunnelRowType.getFieldName(i), element.getField(i));
        }
        String body = objectMapper.writeValueAsString(objectMap);
        try {
            // only support post web hook
            HttpResponse response = httpClient.doPost(httpParameter.getUrl(), httpParameter.getHeaders(), body);
            if (HttpResponse.STATUS_OK == response.getCode()) {
                return;
            }
            LOGGER.error("http client execute exception, http response status code:[{}], content:[{}]", response.getCode(), response.getContent());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }
}

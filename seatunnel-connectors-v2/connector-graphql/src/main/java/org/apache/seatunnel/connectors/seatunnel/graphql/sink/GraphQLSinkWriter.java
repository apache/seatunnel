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
package org.apache.seatunnel.connectors.seatunnel.graphql.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.graphql.config.GraphQLSinkParameter;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkWriter;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class GraphQLSinkWriter extends HttpSinkWriter {
    private final HttpClientProvider httpClient;
    private static final Gson gson = new Gson();
    private Boolean valueCover = false;

    public GraphQLSinkWriter(
            SeaTunnelRowType seaTunnelRowType, GraphQLSinkParameter graphQLSinkParameter) {
        super(seaTunnelRowType, graphQLSinkParameter.getHttpParameter());
        this.httpClient = new HttpClientProvider(httpParameter);
        this.valueCover = graphQLSinkParameter.getValueCover();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String query = httpParameter.getBody().get("query").toString();

        Map<String, Object> variablesTemplate =
                (Map<String, Object>) httpParameter.getBody().get("variables");

        if (variablesTemplate != null) {
            Set<String> vars =
                    variablesTemplate.isEmpty()
                            ? Collections.emptySet()
                            : variablesTemplate.keySet();
            for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                String fieldName = seaTunnelRowType.getFieldName(i);
                Object fieldValue = element.getField(i);

                if (valueCover && vars.contains(fieldName)) {
                    continue;
                }

                variablesTemplate.put(fieldName, fieldValue);
            }
        }

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", query);
        requestBody.put("variables", variablesTemplate);

        String body = gson.toJson(requestBody);

        try {
            HttpResponse response =
                    httpClient.doPost(httpParameter.getUrl(), httpParameter.getHeaders(), body);
            if (HttpResponse.STATUS_OK == response.getCode()) {
                return;
            }
            log.error(
                    "http client execute exception, http response status code:[{}], content:[{}]",
                    response.getCode(),
                    response.getContent());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw CommonError.jsonOperationError("GraphQLSinkWriter", body, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }
}

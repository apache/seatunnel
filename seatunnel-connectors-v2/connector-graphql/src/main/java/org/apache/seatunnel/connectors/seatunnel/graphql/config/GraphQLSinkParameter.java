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

package org.apache.seatunnel.connectors.seatunnel.graphql.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.graphql.util.GraphQLUtil;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GraphQLSinkParameter implements Serializable {
    private final HttpParameter httpParameter;

    private Boolean valueCover = false;

    public GraphQLSinkParameter(ReadonlyConfig pluginConfig) {
        httpParameter = new HttpParameter();
        httpParameter.buildWithConfig(pluginConfig);

        String query = pluginConfig.get(GraphQLSinkOptions.QUERY);
        GraphQLUtil.validateSinkOperation(query);

        GraphQLUtil.validateUrlProtocol(httpParameter.getUrl(), false);
        Map<String, Object> bodymap = new HashMap<>();

        if (pluginConfig.getOptional(GraphQLSinkOptions.VARIABLES).isPresent()) {
            bodymap.put(
                    GraphQLSinkOptions.VARIABLES.key(),
                    pluginConfig.get(GraphQLSinkOptions.VARIABLES));
        } else {
            bodymap.put(GraphQLSinkOptions.VARIABLES.key(), "{}");
        }
        bodymap.put(GraphQLSinkOptions.QUERY.key(), query);
        this.httpParameter.setBody(JsonUtils.toJsonString(bodymap));

        httpParameter.setParams(
                httpParameter.getParams() == null ? new HashMap<>() : httpParameter.getParams());
        httpParameter.setMethod(HttpRequestMethod.POST);

        if (pluginConfig.getOptional(GraphQLSinkOptions.TIMEOUT).isPresent()) {
            this.httpParameter
                    .getParams()
                    .put(
                            GraphQLSinkOptions.TIMEOUT.key(),
                            String.valueOf(pluginConfig.get(GraphQLSinkOptions.TIMEOUT)));
        }

        if (pluginConfig.getOptional(GraphQLSinkOptions.VALUE_COVER).isPresent()) {
            valueCover = pluginConfig.get(GraphQLSinkOptions.VALUE_COVER);
        }
    }

    public HttpParameter getHttpParameter() {
        return httpParameter;
    }

    public Boolean getValueCover() {
        return valueCover;
    }
}

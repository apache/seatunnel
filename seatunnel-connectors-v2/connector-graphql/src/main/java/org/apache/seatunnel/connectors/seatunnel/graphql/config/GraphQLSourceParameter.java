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
import org.apache.seatunnel.connectors.seatunnel.graphql.util.GraphQLUtil;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import java.io.Serializable;
import java.util.HashMap;

public class GraphQLSourceParameter implements Serializable {
    private final HttpParameter httpParameter;
    private Boolean enableSubscription = false;

    private Integer maxRetries = GraphQLSourceOptions.DEFAULT_MAX_RETRIES;
    private Integer retryDelayMs = GraphQLSourceOptions.DEFAULT_RETRY_DELAY_MS;

    public GraphQLSourceParameter(ReadonlyConfig pluginConfig, HttpParameter httpParameter) {
        this.httpParameter = httpParameter;

        if (pluginConfig.getOptional(GraphQLSourceOptions.ENABLE_SUBSCRIPTION).isPresent()) {
            enableSubscription = pluginConfig.get(GraphQLSourceOptions.ENABLE_SUBSCRIPTION);
        }

        String query = pluginConfig.get(GraphQLSourceOptions.QUERY);
        GraphQLUtil.validateSourceOperation(query, enableSubscription);

        GraphQLUtil.validateUrlProtocol(this.httpParameter.getUrl(), enableSubscription);

        this.httpParameter.setBody(new HashMap<>());

        if (pluginConfig.getOptional(GraphQLSourceOptions.VARIABLES).isPresent()) {
            this.httpParameter
                    .getBody()
                    .put(
                            GraphQLSourceOptions.VARIABLES.key(),
                            pluginConfig.get(GraphQLSourceOptions.VARIABLES));
        } else {
            this.httpParameter.getBody().put(GraphQLSourceOptions.VARIABLES.key(), "{}");
        }
        this.httpParameter.getBody().put(GraphQLSourceOptions.QUERY.key(), query);

        this.httpParameter.setParams(
                this.httpParameter.getParams() == null
                        ? new HashMap<>()
                        : this.httpParameter.getParams());
        this.httpParameter.setMethod(HttpRequestMethod.POST);

        if (pluginConfig.getOptional(GraphQLSourceOptions.TIMEOUT).isPresent()) {
            this.httpParameter
                    .getParams()
                    .put(
                            GraphQLSourceOptions.TIMEOUT.key(),
                            String.valueOf(pluginConfig.get(GraphQLSourceOptions.TIMEOUT)));
        }

        if (pluginConfig.getOptional(GraphQLSourceOptions.MAX_RETRIES).isPresent()) {
            maxRetries = pluginConfig.get(GraphQLSourceOptions.MAX_RETRIES);
        }

        if (pluginConfig.getOptional(GraphQLSourceOptions.RETRY_DELAY_MS).isPresent()) {
            retryDelayMs = pluginConfig.get(GraphQLSourceOptions.RETRY_DELAY_MS);
        }
    }

    public Boolean getEnableSubscription() {
        return enableSubscription;
    }

    public HttpParameter getHttpParameter() {
        return httpParameter;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public Integer getRetryDelayMs() {
        return retryDelayMs;
    }
}

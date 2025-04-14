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

package org.apache.seatunnel.connectors.seatunnel.graphql.util;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.graphql.exception.GraphQLConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.graphql.exception.GraphQLConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.DeserializationCollector;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import graphql.language.Document;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class GraphQLUtil {
    private static final Option[] DEFAULT_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL
    };

    private static final Configuration jsonConfiguration =
            Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);

    private static void checkHttpProtocol(String url) {
        checkProtocol(
                url,
                "http://",
                "https://",
                "For non-subscription mode, URL must start with http:// or https://");
    }

    private static void checkProtocol(
            String url, String prefix, String prefix1, String errorMessage) {
        if (!url.startsWith(prefix) && !url.startsWith(prefix1)) {
            throw new GraphQLConnectorException(
                    GraphQLConnectorErrorCode.PROTOCOL_ERROR, errorMessage);
        }
    }

    private static void checkWebSocketProtocol(String url) {
        checkProtocol(
                url,
                "ws://",
                "wss://",
                "For subscription mode, URL must start with ws:// or wss://");
    }

    public static OperationDefinition.Operation parseOperationType(String query) {
        Document document = new Parser().parseDocument(query);
        return document.getDefinitionsOfType(OperationDefinition.class).stream()
                .findFirst()
                .map(OperationDefinition::getOperation)
                .orElse(null);
    }

    public static void validateSinkOperation(String query) {
        if (query == null || query.isEmpty()) {
            throw new GraphQLConnectorException(
                    GraphQLConnectorErrorCode.GRAPHQL_SOURCE_PARAMETER_ERROR,
                    "GraphQL Sink query is required.");
        }
        OperationDefinition.Operation operationType = parseOperationType(query);
        switch (operationType) {
            case MUTATION:
                break;
            case SUBSCRIPTION:
            case QUERY:
            default:
                throw new GraphQLConnectorException(
                        GraphQLConnectorErrorCode.GRAPHQL_SINK_PARAMETER_ERROR,
                        "GraphQL Sink unsupported operation type: " + operationType);
        }
    }

    public static void validateSourceOperation(String query, Boolean enableSubscription) {
        if (query == null) {
            throw new GraphQLConnectorException(
                    GraphQLConnectorErrorCode.GRAPHQL_SOURCE_PARAMETER_ERROR,
                    "GraphQL Source is required.");
        }
        OperationDefinition.Operation operationType;
        try {
            operationType = parseOperationType(query);
        } catch (Exception e) {
            throw new GraphQLConnectorException(
                    GraphQLConnectorErrorCode.GRAPHQL_SOURCE_PARAMETER_ERROR,
                    "Failed to parse operation type from query: " + e.getMessage());
        }
        switch (operationType) {
            case QUERY:
                break;
            case SUBSCRIPTION:
                if (!enableSubscription) {
                    throw new GraphQLConnectorException(
                            GraphQLConnectorErrorCode.GRAPHQL_SOURCE_PARAMETER_ERROR,
                            "Subscription is not enabled.");
                }
                break;
            case MUTATION:
            default:
                throw new GraphQLConnectorException(
                        GraphQLConnectorErrorCode.GRAPHQL_SOURCE_PARAMETER_ERROR,
                        "GraphQL Source unsupported operation type: " + operationType);
        }
    }

    public static void validateUrlProtocol(String url, boolean enableSubscription) {
        if (enableSubscription) {
            checkWebSocketProtocol(url);
        } else {
            checkHttpProtocol(url);
        }
    }

    public static void collect(
            DeserializationCollector deserializationCollector,
            String data,
            String contentJson,
            Collector<SeaTunnelRow> output)
            throws IOException {
        if (data != null && !data.isEmpty()) {
            ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
            if (contentJson != null) {
                Object read = jsonReadContext.read(JsonPath.compile(contentJson));
                if (read != null) {
                    if (read instanceof Object[] || read instanceof List) {
                        Iterable<?> iterable =
                                read instanceof Object[]
                                        ? Arrays.asList((Object[]) read)
                                        : (List<?>) read;
                        for (Object o : iterable) {
                            data = JsonUtils.toJsonString(o);
                            deserializationCollector.collect(data.getBytes(), output);
                        }
                    } else {
                        data = JsonUtils.toJsonString(read);
                        deserializationCollector.collect(data.getBytes(), output);
                    }
                }
            } else {
                String dataJson = JsonUtils.toJsonString(data);
                deserializationCollector.collect(dataJson.getBytes(), output);
            }
        }
    }
}

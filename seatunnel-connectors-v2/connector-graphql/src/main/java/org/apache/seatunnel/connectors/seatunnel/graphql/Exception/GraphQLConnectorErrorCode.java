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

package org.apache.seatunnel.connectors.seatunnel.graphql.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum GraphQLConnectorErrorCode implements SeaTunnelErrorCode {
    GRAPHQL_SOURCE_PARAMETER_ERROR("GraphQL-00", "The parameter of GraphQL is error"),
    GRAPHQL_SOURCE_OPERATION_ERROR("GraphQL-01", "The operation of GraphQL is error"),
    GRAPHQL_SINK_PARAMETER_ERROR("GraphQL-02", "The parameter of GraphQL is error"),
    GRAPHQL_SINK_OPERATION_ERROR("GraphQL-03", "The operation of GraphQL is error"),
    PROTOCOL_ERROR("GraphQL-04", "The protocol of GraphQL is error"),
    GRAPHQL_RESPONSE_NULL_DATA("GraphQL-05", "The response of GraphQL is null");

    private final String code;
    private final String description;

    GraphQLConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

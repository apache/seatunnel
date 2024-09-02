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

package org.apache.seatunnel.connectors.seatunnel.typesense.util;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class URLParamsConverter {

    public static String convertParamsToJson(String paramsString) {
        return Optional.ofNullable(paramsString)
                .filter(s -> !s.isEmpty())
                .map(URLParamsConverter::parseParams)
                .map(
                        paramsMap -> {
                            try {
                                return new ObjectMapper().writeValueAsString(paramsMap);
                            } catch (IOException e) {
                                throw new RuntimeException("Error converting params to JSON", e);
                            }
                        })
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Parameter string must not be null or empty."));
    }

    private static Map<String, String> parseParams(String paramsString) {
        return Arrays.stream(
                        Optional.ofNullable(paramsString)
                                .filter(s -> !s.isEmpty())
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Parameter string must not be null or empty."))
                                .split("&"))
                .map(part -> part.split("=", 2))
                .peek(
                        keyValue -> {
                            if (keyValue.length != 2) {
                                throw new TypesenseConnectorException(
                                        TypesenseConnectorErrorCode.QUERY_PARAM_ERROR,
                                        "Query parameter error: " + Arrays.toString(keyValue));
                            }
                        })
                .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));
    }
}

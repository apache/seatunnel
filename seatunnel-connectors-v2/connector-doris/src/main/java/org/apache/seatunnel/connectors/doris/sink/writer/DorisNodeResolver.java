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

package org.apache.seatunnel.connectors.doris.sink.writer;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for parsing FE/BE host lists used by Doris stream load. */
public final class DorisNodeResolver {

    private DorisNodeResolver() {}

    public static List<String> parseNodes(String rawNodes, String optionName) {
        if (StringUtils.isBlank(rawNodes)) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: Doris, Message: Option '%s' cannot be blank.",
                            optionName));
        }

        return Arrays.stream(rawNodes.split(","))
                .map(String::trim)
                .peek(node -> validateNode(node, optionName))
                .collect(Collectors.toList());
    }

    private static void validateNode(String node, String optionName) {
        if (StringUtils.isBlank(node)) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: Doris, Message: Option '%s' contains a blank node entry.",
                            optionName));
        }
        int splitIndex = node.lastIndexOf(':');
        if (splitIndex <= 0 || splitIndex == node.length() - 1) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: Doris, Message: Option '%s' contains invalid host:port value '%s'.",
                            optionName, node));
        }
        try {
            int port = Integer.parseInt(node.substring(splitIndex + 1));
            if (port <= 0) {
                throw new NumberFormatException("port must be positive");
            }
        } catch (NumberFormatException e) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: Doris, Message: Option '%s' contains invalid host:port value '%s'.",
                            optionName, node));
        }
    }
}

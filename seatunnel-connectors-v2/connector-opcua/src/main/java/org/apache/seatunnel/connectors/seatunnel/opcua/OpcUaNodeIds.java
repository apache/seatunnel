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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

import java.util.Base64;
import java.util.UUID;

/** Validates textual identifiers and resolves them against the current server namespace table. */
final class OpcUaNodeIds {
    private static final int MAX_NODE_ID_LENGTH = 4096;

    private OpcUaNodeIds() {}

    static ExpandedNodeId parse(String input) {
        if (input == null
                || input.length() > MAX_NODE_ID_LENGTH
                || input.isBlank()
                || input.indexOf('\0') >= 0) {
            throw OpcUaSettings.invalid("invalid textual NodeId");
        }
        try {
            String body = input;
            if (input.startsWith("ns=")) {
                int end = input.indexOf(';');
                if (end < 4
                        || !input.substring(3, end).matches("[0-9]{1,5}")
                        || Integer.parseInt(input.substring(3, end)) > 65535) {
                    throw new IllegalArgumentException();
                }
                body = input.substring(end + 1);
            } else if (input.startsWith("nsu=")) {
                int end = input.indexOf(';');
                if (end < 5) {
                    throw new IllegalArgumentException();
                }
                body = input.substring(end + 1);
            }
            if (body.length() < 3 || body.charAt(1) != '=') {
                throw new IllegalArgumentException();
            }
            String identifier = body.substring(2);
            switch (body.charAt(0)) {
                case 'i':
                    if (!identifier.matches("[0-9]{1,10}")
                            || Long.parseLong(identifier) > 4294967295L) {
                        throw new IllegalArgumentException();
                    }
                    break;
                case 'g':
                    if (!UUID.fromString(identifier).toString().equalsIgnoreCase(identifier)) {
                        throw new IllegalArgumentException();
                    }
                    break;
                case 'b':
                    if (Base64.getDecoder().decode(identifier).length == 0) {
                        throw new IllegalArgumentException();
                    }
                    break;
                case 's':
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            ExpandedNodeId parsed = ExpandedNodeId.parse(input);
            if (parsed.getServerIndex().longValue() != 0L) {
                throw new IllegalArgumentException();
            }
            return parsed;
        } catch (RuntimeException e) {
            throw OpcUaSettings.invalid("invalid textual NodeId");
        }
    }

    static NodeId resolve(String text, NamespaceTable namespaces) throws OpcUaFailure {
        NodeId local =
                parse(text)
                        .local(namespaces)
                        .orElseThrow(
                                () ->
                                        OpcUaFailure.problem(
                                                "NodeId namespace is not present on the server"));
        if (namespaces.getUri(local.getNamespaceIndex()) == null || local.isNull()) {
            throw OpcUaFailure.problem("NodeId is null or its namespace is not registered");
        }
        return local;
    }

    static String namespaceIdentity(NodeId local, NamespaceTable namespaces) throws OpcUaFailure {
        String uri = namespaces.getUri(local.getNamespaceIndex());
        if (uri == null || uri.indexOf(';') >= 0) {
            throw OpcUaFailure.problem(
                    "server returned a NodeId with an unregistered or ambiguous namespace");
        }
        String identifier = local.toParseableString();
        int semicolon = identifier.indexOf(';');
        if (identifier.startsWith("ns=")) {
            identifier = identifier.substring(semicolon + 1);
        }
        String canonical = "nsu=" + uri + ";" + identifier;
        if (canonical.length() > MAX_NODE_ID_LENGTH || canonical.indexOf('\0') >= 0) {
            throw OpcUaFailure.problem(
                    "server NodeId exceeds its textual length or character limit");
        }
        return canonical;
    }
}

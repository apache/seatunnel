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

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ViewDescription;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaFailure.problem;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

/** Breadth-first browsing with explicit continuation ownership and numeric resource bounds. */
final class OpcUaBrowse {
    private static final int MAX_PAGES = 20000;
    private final OpcUaSettings settings;
    private final OpcUaClient client;
    private final NamespaceTable namespaces;
    private final long deadline;

    private final Set<NodeId> visited = new HashSet<>();
    private final List<Entry> entries = new ArrayList<>();
    private int pageCount;
    private long retainedBytes;
    private long referenceBytes;

    OpcUaBrowse(OpcUaSettings settings, OpcUaClient client, long deadline) {
        this.settings = settings;
        this.client = client;
        this.namespaces = client.getNamespaceTable();
        this.deadline = deadline;
    }

    List<Entry> walk(NodeId root) throws Exception {
        Deque<Entry> pending = new ArrayDeque<>();
        visited.add(root);
        pending.add(
                new Entry(
                        root,
                        OpcUaNodeIds.namespaceIdentity(root, namespaces),
                        null,
                        null,
                        null,
                        0));
        while (!pending.isEmpty()) {
            Entry branch = pending.removeFirst();
            collect(branch, pending);
        }
        return entries;
    }

    private void collect(Entry branch, Deque<Entry> pending) throws Exception {
        BrowseDescription operation =
                new BrowseDescription(
                        branch.local,
                        BrowseDirection.Forward,
                        Identifiers.References,
                        true,
                        uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
                        uint(BrowseResultMask.All.getValue()));
        BrowseResult result =
                one(
                        OpcUaSession.await(
                                        client.browse(
                                                new ViewDescription(
                                                        NodeId.NULL_VALUE,
                                                        DateTime.NULL_VALUE,
                                                        uint(0)),
                                                uint(settings.pageSize),
                                                List.of(operation)),
                                        deadline,
                                        "Browse")
                                .getResults());
        ByteString held = null;
        Set<List<Object>> references = new HashSet<>();
        referenceBytes = 0L;
        try {
            for (; ; ) {
                ByteString next = result.getContinuationPoint();
                held = next;
                if (result.getStatusCode() == null || !result.getStatusCode().isGood()) {
                    throw problem("Browse returned a non-good node status");
                }
                if (++pageCount > MAX_PAGES) {
                    throw problem("Browse exceeded its continuation page limit");
                }
                int before = references.size();
                ReferenceDescription[] children = result.getReferences();
                if (children != null) {
                    for (ReferenceDescription ref : children) {
                        addChild(branch, pending, references, ref);
                    }
                }
                if (!present(next)) {
                    return;
                }
                if (references.size() == before) {
                    throw problem("Browse continuation did not make progress");
                }
                result =
                        one(
                                OpcUaSession.await(
                                                client.browseNext(false, List.of(next)),
                                                deadline,
                                                "BrowseNext")
                                        .getResults());
            }
        } finally {
            if (present(held) && !Thread.currentThread().isInterrupted()) {
                try {
                    OpcUaSession.await(
                            client.browseNext(true, List.of(held)),
                            System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250),
                            "release Browse continuation");
                } catch (OpcUaFailure ignored) {
                    // The client/session shutdown also releases continuation points.
                }
            }
        }
    }

    private void addChild(
            Entry branch,
            Deque<Entry> pending,
            Set<List<Object>> references,
            ReferenceDescription ref)
            throws OpcUaFailure {
        if (ref == null
                || ref.getNodeId() == null
                || ref.getReferenceTypeId() == null
                || ref.getNodeClass() == null) {
            throw problem("Browse returned incomplete reference metadata");
        }
        ExpandedNodeId target = ref.getNodeId();
        NodeId type = ref.getReferenceTypeId();
        if (references.add(List.of(target, type))) {
            referenceBytes +=
                    160L
                            + identifierWeight(target.getIdentifier())
                            + identifierWeight(type.getIdentifier())
                            + textWeight(target.getNamespaceUri());
        }
        if (references.size() > MAX_PAGES) {
            throw problem("Browse exceeded its per-node reference limit");
        }
        checkMetadataBudget();
        if (target.getServerIndex().longValue() != 0L) {
            return;
        }
        NodeId node =
                target.local(namespaces)
                        .orElseThrow(() -> problem("Browse reference namespace is not registered"));
        if (!visited.add(node)) {
            return;
        }
        if (visited.size() - 1 > settings.maxNodes || branch.depth >= settings.maxDepth) {
            throw problem("Browse exceeded max_nodes or max_depth");
        }
        String name = ref.getBrowseName() == null ? null : ref.getBrowseName().getName();
        String identity = OpcUaNodeIds.namespaceIdentity(node, namespaces);
        retainedBytes +=
                256L
                        + textWeight(identity)
                        + textWeight(name)
                        + identifierWeight(node.getIdentifier());
        checkMetadataBudget();
        Entry entry =
                new Entry(
                        node,
                        identity,
                        branch.identity,
                        name,
                        ref.getNodeClass(),
                        branch.depth + 1);
        entries.add(entry);
        pending.addLast(entry);
    }

    private void checkMetadataBudget() throws OpcUaFailure {
        if (retainedBytes + referenceBytes > OpcUaRows.MAX_BUFFER_BYTES) {
            throw problem("Browse exceeded its metadata buffer limit");
        }
    }

    private static long identifierWeight(Object identifier) {
        if (identifier instanceof ByteString) {
            return ((ByteString) identifier).length();
        }
        return identifier instanceof String ? textWeight((String) identifier) : 0L;
    }

    private static long textWeight(String text) {
        return text == null ? 0L : 2L * text.length();
    }

    private static boolean present(ByteString point) {
        return point != null && point.length() != 0;
    }

    private static BrowseResult one(BrowseResult[] results) throws OpcUaFailure {
        if (results == null || results.length != 1 || results[0] == null) {
            throw problem("Browse did not return exactly one result");
        }
        return results[0];
    }

    static final class Entry {
        final NodeId local;
        final String identity;
        final String parent;
        final String name;
        final NodeClass kind;
        final int depth;

        private Entry(
                NodeId local,
                String identity,
                String parent,
                String name,
                NodeClass kind,
                int depth) {
            this.local = local;
            this.identity = identity;
            this.parent = parent;
            this.name = name;
            this.kind = kind;
            this.depth = depth;
        }
    }
}

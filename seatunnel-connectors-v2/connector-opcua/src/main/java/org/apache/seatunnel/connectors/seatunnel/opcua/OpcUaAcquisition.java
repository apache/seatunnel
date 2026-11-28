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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.SessionActivityListener;
import org.eclipse.milo.opcua.sdk.client.api.UaSession;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscriptionManager;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaFailure.problem;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSession.await;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

/** Worker-owned data requests with a bounded queue for SDK subscription callbacks. */
final class OpcUaAcquisition implements AutoCloseable {

    private final OpcUaSettings settings;
    private final OpcUaSession session;
    private final OpcUaRows rows;
    // Keep each configured identifier bound to its first resolved namespace URI.
    private final Map<String, String> binding = new LinkedHashMap<>();
    private final Deque<QueuedRow> notifications = new ArrayDeque<>();
    private final AtomicReference<OpcUaFailure> fatal = new AtomicReference<>();
    private final AtomicReference<String> recreate = new AtomicReference<>();
    private final AtomicLong inactiveSince = new AtomicLong();
    // Old subscription callbacks may still arrive after installing the replacement.
    private final AtomicLong generation = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private int queuedBytes;
    private long refreshAt;
    private boolean restoredBinding;
    private List<Target> targets;
    private OpcUaClient targetClient;
    private volatile UaSubscription subscription;

    OpcUaAcquisition(OpcUaSettings settings, String tableId) {
        this.settings = settings;
        this.rows = new OpcUaRows(settings, tableId);
        session = new OpcUaSession(settings);
    }

    Batch next(Map<String, String> saved, long deadline) throws Exception {
        if (!restoredBinding) {
            binding.putAll(saved);
            restoredBinding = true;
        }
        requireHealthy();
        if (settings.mode == OpcUaSettings.ReadMode.BROWSE_NODES) {
            OpcUaClient client = session.connect(deadline);
            List<SeaTunnelRow> records = new ArrayList<>();
            int bytes = 0;
            for (OpcUaBrowse.Entry entry : browse(client, deadline)) {
                SeaTunnelRow row =
                        rows.node(
                                entry.identity, entry.parent, entry.name, entry.kind, entry.depth);
                bytes = append(records, row, bytes);
            }
            return new Batch(records, binding);
        }
        if (settings.mode == OpcUaSettings.ReadMode.POLLING) {
            OpcUaClient client = session.connect(deadline);
            List<SeaTunnelRow> records;
            try {
                records = poll(client, deadline);
            } catch (OpcUaFailure e) {
                if (!e.retryable
                        || deadline - System.nanoTime() < TimeUnit.MILLISECONDS.toNanos(100)) {
                    throw e;
                }
                session.reset();
                records = poll(session.connect(deadline), deadline);
            }
            return new Batch(records, binding);
        }
        return subscribe(deadline);
    }

    private List<SeaTunnelRow> poll(OpcUaClient client, long deadline) throws Exception {
        selectTargets(client, deadline);
        List<SeaTunnelRow> records = new ArrayList<>();
        int bytes = 0;
        for (int first = 0; first < targets.size(); first += settings.batchSize) {
            List<Target> part =
                    targets.subList(first, Math.min(first + settings.batchSize, targets.size()));
            List<NodeId> ids =
                    part.stream().map(target -> target.local).collect(Collectors.toList());
            List<DataValue> values =
                    await(
                            client.readValues(0.0, TimestampsToReturn.Both, ids),
                            deadline,
                            "Read values");
            if (values == null || values.size() != part.size()) {
                throw problem("Read returned a different number of values than requested");
            }
            for (int i = 0; i < part.size(); i++) {
                Target target = part.get(i);
                SeaTunnelRow row = rows.value(target.identity, target.alias, values.get(i));
                bytes = append(records, row, bytes);
            }
        }
        return records;
    }

    private static int append(List<SeaTunnelRow> records, SeaTunnelRow row, int bytes)
            throws OpcUaFailure {
        int total = bytes + OpcUaRows.weight(row);
        if (total > OpcUaRows.MAX_BUFFER_BYTES) {
            throw problem("OPC UA read exceeds the result buffer limit");
        }
        records.add(row);
        return total;
    }

    private Batch subscribe(long deadline) throws Exception {
        OpcUaClient client = session.connect(deadline);
        long inactive = inactiveSince.get();
        if (inactive != 0L
                && System.nanoTime() - inactive
                        > TimeUnit.MILLISECONDS.toNanos(settings.sessionTimeout)) {
            throw problem("OPC UA session did not recover within session_timeout_ms");
        }
        String recovery = recreate.getAndSet(null);
        if (inactive == 0L) {
            boolean first = targetClient != client;
            boolean changed = selectTargets(client, deadline);
            if (first) {
                listeners(client);
            }
            if (subscription == null || changed || recovery != null) {
                rebuild(
                        client,
                        recovery != null
                                ? recovery
                                : subscription == null ? null : "NODE_SELECTION_CHANGED",
                        deadline);
            }
        } else if (recovery != null) {
            recreate.compareAndSet(null, recovery);
        }
        requireHealthy();
        return new Batch(drain(), binding);
    }

    private boolean selectTargets(OpcUaClient client, long deadline) throws Exception {
        boolean newClient = targetClient != client;
        NamespaceTable namespaces = client.getNamespaceTable();
        if (!newClient
                && sameNamespaceMapping(namespaces)
                && (settings.fetch != OpcUaSettings.FetchMode.BROWSE
                        || System.nanoTime() - refreshAt < 0L)) {
            return false;
        }
        List<Target> selected = new ArrayList<>();
        if (settings.fetch == OpcUaSettings.FetchMode.BROWSE) {
            for (OpcUaBrowse.Entry entry : browse(client, deadline)) {
                if (entry.kind == NodeClass.Variable) {
                    selected.add(
                            new Target(
                                    entry.local,
                                    entry.identity,
                                    entry.name == null ? entry.identity : entry.name));
                }
            }
            refreshAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.refreshInterval);
        } else {
            Set<NodeId> seen = new HashSet<>();
            for (Map.Entry<String, String> node : settings.nodes.entrySet()) {
                NodeId id = bind(namespaces, node.getValue());
                if (!seen.add(id)) {
                    throw problem("node selection resolves to a duplicate NodeId");
                }
                selected.add(
                        new Target(
                                id, OpcUaNodeIds.namespaceIdentity(id, namespaces), node.getKey()));
            }
        }
        if (selected.isEmpty()) {
            throw problem("node selection did not contain any variable nodes");
        }
        boolean changed = newClient || targets.size() != selected.size();
        if (!changed) {
            for (int i = 0; i < selected.size(); i++) {
                Target old = targets.get(i);
                Target next = selected.get(i);
                if (!old.identity.equals(next.identity)
                        || !old.alias.equals(next.alias)
                        || !old.local.equals(next.local)) {
                    changed = true;
                    break;
                }
            }
        }
        targets = selected;
        targetClient = client;
        return changed;
    }

    private boolean sameNamespaceMapping(NamespaceTable namespaces) throws OpcUaFailure {
        for (Target target : targets) {
            if (!target.local.equals(OpcUaNodeIds.resolve(target.identity, namespaces))) {
                return false;
            }
        }
        return true;
    }

    private List<OpcUaBrowse.Entry> browse(OpcUaClient client, long deadline) throws Exception {
        NodeId root = bind(client.getNamespaceTable(), settings.root);
        return new OpcUaBrowse(settings, client, deadline).walk(root);
    }

    private NodeId bind(NamespaceTable namespaces, String configured) throws OpcUaFailure {
        String assigned = binding.get(configured);
        NodeId node = OpcUaNodeIds.resolve(assigned == null ? configured : assigned, namespaces);
        if (assigned == null) {
            binding.put(configured, OpcUaNodeIds.namespaceIdentity(node, namespaces));
        }
        return node;
    }

    private void listeners(OpcUaClient client) {
        client.addSessionActivityListener(
                new SessionActivityListener() {
                    @Override
                    public void onSessionInactive(UaSession session) {
                        inactiveSince.compareAndSet(0L, System.nanoTime());
                    }

                    @Override
                    public void onSessionActive(UaSession session) {
                        if (inactiveSince.getAndSet(0L) != 0L && subscription != null) {
                            // Successful transfer and server replay are SDK-managed and not durable
                            // history.
                            offer(rows.gap("SESSION_INTERRUPTED"));
                        }
                    }
                });
        client.getSubscriptionManager()
                .addSubscriptionListener(
                        new UaSubscriptionManager.SubscriptionListener() {
                            @Override
                            public void onNotificationDataLost(UaSubscription affected) {
                                if (affected == subscription) {
                                    offer(rows.gap("NOTIFICATION_DATA_LOST"));
                                }
                            }

                            @Override
                            public void onSubscriptionTransferFailed(
                                    UaSubscription affected, StatusCode status) {
                                if (affected == subscription) {
                                    recreate.compareAndSet(null, "SUBSCRIPTION_TRANSFER_FAILED");
                                }
                            }

                            @Override
                            public void onStatusChanged(
                                    UaSubscription affected, StatusCode status) {
                                if (affected == subscription && status != null && status.isBad()) {
                                    recreate.compareAndSet(null, "SUBSCRIPTION_STATUS_BAD");
                                }
                            }

                            @Override
                            public void onSubscriptionWatchdogTimerElapsed(
                                    UaSubscription affected) {
                                if (affected == subscription) {
                                    recreate.compareAndSet(null, "SUBSCRIPTION_WATCHDOG");
                                }
                            }
                        });
    }

    private void rebuild(OpcUaClient client, String reason, long deadline) throws OpcUaFailure {
        long epoch = generation.incrementAndGet();
        UaSubscription old = subscription;
        subscription = null;
        UaSubscriptionManager manager = client.getSubscriptionManager();
        if (old != null && manager.getSubscriptions().contains(old)) {
            await(
                    manager.deleteSubscription(old.getSubscriptionId()),
                    deadline,
                    "Delete subscription");
        }
        if (reason != null) {
            offer(rows.gap(reason));
        }
        UaSubscription next =
                await(
                        manager.createSubscription(settings.interval),
                        deadline,
                        "Create subscription");
        subscription = next;
        int handle = 1;
        for (int first = 0; first < targets.size(); first += settings.batchSize) {
            List<Target> part =
                    targets.subList(first, Math.min(first + settings.batchSize, targets.size()));
            List<MonitoredItemCreateRequest> requests = new ArrayList<>();
            for (Target target : part) {
                ReadValueId value =
                        new ReadValueId(
                                target.local,
                                AttributeId.Value.uid(),
                                null,
                                QualifiedName.NULL_VALUE);
                MonitoringParameters parameters =
                        new MonitoringParameters(
                                uint(handle++),
                                (double) settings.interval,
                                null,
                                uint(Math.min(10, settings.queueCapacity)),
                                true);
                requests.add(
                        new MonitoredItemCreateRequest(
                                value, MonitoringMode.Reporting, parameters));
            }
            List<UaMonitoredItem> created =
                    await(
                            next.createMonitoredItems(
                                    TimestampsToReturn.Both,
                                    requests,
                                    (item, index) ->
                                            item.setValueConsumer(
                                                    (ignored, value) ->
                                                            consume(
                                                                    epoch,
                                                                    part.get(index),
                                                                    value))),
                            deadline,
                            "Create monitored items");
            if (created == null
                    || created.size() != part.size()
                    || created.stream()
                            .anyMatch(
                                    item ->
                                            item == null
                                                    || item.getStatusCode() == null
                                                    || !item.getStatusCode().isGood())) {
                throw problem("server rejected one or more monitored items");
            }
        }
    }

    private void consume(long epoch, Target target, DataValue value) {
        if (epoch != generation.get() || closed.get()) {
            return;
        }
        try {
            if (value != null
                    && value.getStatusCode() != null
                    && value.getStatusCode().isOverflowSet()) {
                offer(rows.gap("SERVER_MONITORED_ITEM_QUEUE_OVERFLOW"));
            }
            offer(rows.value(target.identity, target.alias, value));
        } catch (OpcUaFailure failure) {
            fatal.compareAndSet(null, failure);
        }
    }

    private synchronized void offer(SeaTunnelRow row) {
        if (closed.get() || fatal.get() != null) {
            return;
        }
        int weight = OpcUaRows.weight(row);
        if (notifications.size() == settings.queueCapacity
                || weight > OpcUaRows.MAX_BUFFER_BYTES - queuedBytes) {
            fatal.compareAndSet(null, problem("subscription notification queue overflow"));
            return;
        }
        notifications.addLast(new QueuedRow(row, weight));
        queuedBytes += weight;
    }

    private synchronized List<SeaTunnelRow> drain() {
        List<SeaTunnelRow> result =
                new ArrayList<>(Math.min(notifications.size(), settings.batchSize));
        while (result.size() < settings.batchSize && !notifications.isEmpty()) {
            QueuedRow entry = notifications.removeFirst();
            queuedBytes -= entry.weight;
            result.add(entry.row);
        }
        return result;
    }

    private void requireHealthy() throws OpcUaFailure {
        OpcUaFailure failure = fatal.get();
        if (failure != null) {
            throw failure;
        }
        session.validateHealth();
    }

    /** Ignore further callbacks and close the private session once. */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            session.close();
        }
    }

    static final class Batch {
        final List<SeaTunnelRow> rows;
        final Map<String, String> binding;

        private Batch(List<SeaTunnelRow> rows, Map<String, String> binding) {
            this.rows = rows;
            this.binding = new LinkedHashMap<>(binding);
        }
    }

    private static final class Target {
        private final NodeId local;
        private final String identity;
        private final String alias;

        private Target(NodeId local, String identity, String alias) {
            this.local = local;
            this.identity = identity;
            this.alias = alias;
        }
    }

    private static final class QueuedRow {
        private final SeaTunnelRow row;
        private final int weight;

        private QueuedRow(SeaTunnelRow row, int weight) {
            this.row = row;
            this.weight = weight;
        }
    }
}

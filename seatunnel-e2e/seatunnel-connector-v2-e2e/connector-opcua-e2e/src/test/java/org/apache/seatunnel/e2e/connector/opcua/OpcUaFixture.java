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

package org.apache.seatunnel.e2e.connector.opcua;

import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.DataItem;
import org.eclipse.milo.opcua.sdk.server.api.ManagedNamespaceWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.transport.TransportProfile;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.server.EndpointConfiguration;
import org.eclipse.milo.opcua.stack.server.security.DefaultServerCertificateValidator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ulong;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ushort;

/** Native UA server run in a dedicated JDK 11 container. HTTP is used only by the test driver. */
public final class OpcUaFixture {
    static final String HOST = "opcua-fixture";
    static final String NS = "urn:seatunnel:opcua:fixture:nodes";
    static final String AUX_NS = "urn:seatunnel:opcua:fixture:aux";
    static final String SERVER_URI = "urn:seatunnel:opcua:fixture:server";
    static final String CLIENT_URI = "urn:seatunnel:opcua:fixture:client";
    static final String GUID = "8c1e7991-bfaf-49ef-ab1a-1f63816105ea";
    static final String PASSWORD = "test-keystore-only";
    static final int UA = 4840;
    static final int SILENT = 4841;
    static final int CONTROL = 8080;

    private final AtomicInteger silentConnections = new AtomicInteger();
    private final AtomicInteger activeSilentConnections = new AtomicInteger();
    private final DefaultTrustListManager trust;
    private final OpcUaServer server;
    private final FixtureNamespace namespace;

    private OpcUaFixture(Path serverKeys, Path clientKeys) throws Exception {
        KeyStore keyStore = load(serverKeys);
        X509Certificate certificate = (X509Certificate) keyStore.getCertificate("test");
        PrivateKey privateKey = (PrivateKey) keyStore.getKey("test", PASSWORD.toCharArray());
        trust = new DefaultTrustListManager(Files.createTempDirectory("opcua-trust").toFile());
        trust.addTrustedCertificate((X509Certificate) load(clientKeys).getCertificate("test"));
        server =
                new OpcUaServer(
                        OpcUaServerConfig.builder()
                                .setApplicationUri(SERVER_URI)
                                .setApplicationName(LocalizedText.english("SeaTunnel OPC UA E2E"))
                                .setProductUri(SERVER_URI)
                                .setCertificateManager(
                                        new DefaultCertificateManager(
                                                new KeyPair(certificate.getPublicKey(), privateKey),
                                                certificate))
                                .setTrustListManager(trust)
                                .setCertificateValidator(
                                        new DefaultServerCertificateValidator(trust))
                                .setEndpoints(
                                        Set.of(
                                                endpoint(
                                                        certificate,
                                                        HOST,
                                                        SecurityPolicy.None,
                                                        MessageSecurityMode.None),
                                                endpoint(
                                                        certificate,
                                                        HOST,
                                                        SecurityPolicy.Basic256Sha256,
                                                        MessageSecurityMode.Sign),
                                                endpoint(
                                                        certificate,
                                                        HOST,
                                                        SecurityPolicy.Basic256Sha256,
                                                        MessageSecurityMode.SignAndEncrypt),
                                                endpoint(
                                                        certificate,
                                                        "opcua-alias",
                                                        SecurityPolicy.None,
                                                        MessageSecurityMode.None),
                                                endpoint(
                                                        certificate,
                                                        "opcua-alias",
                                                        SecurityPolicy.Basic256Sha256,
                                                        MessageSecurityMode.SignAndEncrypt),
                                                endpoint(
                                                        certificate,
                                                        HOST,
                                                        "/secure",
                                                        SecurityPolicy.Basic256Sha256,
                                                        MessageSecurityMode.SignAndEncrypt),
                                                endpoint(
                                                        certificate,
                                                        HOST,
                                                        "/secure/discovery",
                                                        SecurityPolicy.None,
                                                        MessageSecurityMode.None)))
                                .build());
        namespace = new FixtureNamespace(server, NS, true);
        namespace.startup();
        // Same identifier in another namespace proves selection does not key on the bare
        // identifier.
        new FixtureNamespace(server, AUX_NS, false).startup();
        server.startup().get(30, TimeUnit.SECONDS);
        new Thread(this::silentServer, "opcua-silent-peer").start();
    }

    private static EndpointConfiguration endpoint(
            X509Certificate certificate,
            String hostname,
            SecurityPolicy policy,
            MessageSecurityMode mode) {
        return endpoint(certificate, hostname, "/fixture", policy, mode);
    }

    private static EndpointConfiguration endpoint(
            X509Certificate certificate,
            String hostname,
            String path,
            SecurityPolicy policy,
            MessageSecurityMode mode) {
        return EndpointConfiguration.newBuilder()
                .setBindAddress("0.0.0.0")
                .setBindPort(UA)
                .setHostname(hostname)
                .setPath(path)
                .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
                .setCertificate(certificate)
                .setSecurityPolicy(policy)
                .setSecurityMode(mode)
                .addTokenPolicies(OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS)
                .build();
    }

    static KeyStore load(Path path) throws Exception {
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(path)) {
            key.load(in, PASSWORD.toCharArray());
        }
        return key;
    }

    public static void main(String[] args) throws Exception {
        if (Runtime.version().feature() != 11) {
            throw new IllegalStateException("The OPC UA fixture needs JDK 11");
        }
        OpcUaFixture fixture = new OpcUaFixture(Path.of(args[0]), Path.of(args[1]));
        HttpServer control = HttpServer.create(new InetSocketAddress(CONTROL), 8);
        control.createContext("/", fixture::control);
        control.start();
        System.out.println("OPC UA fixture ready");
        new CountDownLatch(1).await();
    }

    private void control(HttpExchange exchange) throws IOException {
        String[] command = exchange.getRequestURI().getPath().substring(1).split("/");
        int status = 200;
        String result = "ok";
        switch (command[0]) {
            case "sample":
                namespace.sample.setValue(new DataValue(new Variant(Integer.valueOf(command[1]))));
                break;
            case "extend":
                namespace.extend();
                break;
            case "disconnect":
                server.getStackServer()
                        .getConnectedChannels()
                        .forEach(channel -> channel.close().awaitUninterruptibly(2000));
                break;
            case "rejected":
                result =
                        trust.getRejectedCertificates().stream()
                                .map(cert -> cert.getSerialNumber().toString(16))
                                .collect(Collectors.joining("\n"));
                break;
            case "counts":
                result =
                        server.getSessionManager().getAllSessions().size()
                                + ","
                                + silentConnections.get()
                                + ","
                                + activeSilentConnections.get()
                                + ","
                                + namespace.monitored.get();
                break;
            default:
                status = 404;
                result = "unknown";
        }
        byte[] bytes = result.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private void silentServer() {
        try (ServerSocket listener = new ServerSocket(SILENT)) {
            for (; ; ) {
                try (Socket peer = listener.accept()) {
                    activeSilentConnections.incrementAndGet();
                    silentConnections.incrementAndGet();
                    try {
                        peer.setSoTimeout(40000);
                        while (peer.getInputStream().read() != -1) {
                            // Consume the Hello but never acknowledge it.
                        }
                    } catch (IOException ignored) {
                        // Client cancellation and timeout close the socket.
                    } finally {
                        activeSilentConnections.decrementAndGet();
                    }
                }
            }
        } catch (IOException failure) {
            throw new IllegalStateException("Could not open silent fixture endpoint", failure);
        }
    }

    private static final class FixtureNamespace extends ManagedNamespaceWithLifecycle {
        private final SubscriptionModel subscriptions;
        private final AtomicInteger monitored = new AtomicInteger();
        private final boolean primary;
        private UaFolderNode tree;
        private UaVariableNode sample;

        private FixtureNamespace(OpcUaServer server, String uri, boolean primary) {
            super(server, uri);
            this.primary = primary;
            subscriptions = new SubscriptionModel(server, this);
            getLifecycleManager().addLifecycle(subscriptions);
            getLifecycleManager().addStartupTask(this::populate);
        }

        private void populate() {
            tree = folder("tree");
            if (!primary) {
                sample =
                        variable(
                                tree,
                                newNodeId("sample"),
                                "other",
                                Identifiers.Int32,
                                new DataValue(new Variant(77)));
                return;
            }
            UaFolderNode nested = folder("nested");
            tree.addOrganizes(nested);
            nested.addOrganizes(tree);
            sample =
                    variable(
                            tree,
                            newNodeId("sample"),
                            "current",
                            Identifiers.Int32,
                            new DataValue(new Variant(3)));
            variable(
                    tree,
                    newNodeId(42L),
                    "unsigned",
                    Identifiers.UInt64,
                    new DataValue(
                            new Variant(ulong("18446744073709551615")),
                            StatusCode.GOOD,
                            new DateTime(Instant.parse("1969-12-31T23:59:59.999999900Z")),
                            ushort(73),
                            new DateTime(Instant.parse("2024-01-01T00:00:00.123456700Z")),
                            ushort(29)));
            variable(
                    nested,
                    newNodeId(UUID.fromString(GUID)),
                    "words",
                    Identifiers.String,
                    new DataValue(new Variant(new String[] {"水", null, "café"})));
            variable(
                    nested,
                    newNodeId(ByteString.of(new byte[] {1, 2, 3})),
                    "bytes",
                    Identifiers.ByteString,
                    new DataValue(new Variant(ByteString.of(new byte[] {0, -1, 2}))));
            variable(
                    tree,
                    newNodeId("bad"),
                    "bad",
                    Identifiers.Int32,
                    new DataValue(new Variant(42), new StatusCode(StatusCodes.Bad_NotReadable)));
            folder("empty");
            UaFolderNode longIdentity = folder("long-identity");
            variable(
                    longIdentity,
                    newNodeId("normal"),
                    "normal",
                    Identifiers.Int32,
                    new DataValue(new Variant(1)));
            variable(
                    longIdentity,
                    newNodeId("long".repeat(2048)),
                    "too long",
                    Identifiers.Int32,
                    new DataValue(new Variant(2)));
            UaFolderNode burst = folder("burst");
            for (int i = 0; i < 32; i++) {
                variable(
                        burst,
                        newNodeId("burst" + i),
                        "burst" + i,
                        Identifiers.Int32,
                        new DataValue(new Variant(i)));
            }
        }

        private void extend() {
            NodeId id = newNodeId("later");
            if (!getNodeManager().getNode(id).isPresent()) {
                variable(tree, id, "later", Identifiers.Int32, new DataValue(new Variant(99)));
            }
        }

        private UaFolderNode folder(String id) {
            UaFolderNode node =
                    new UaFolderNode(
                            getNodeContext(),
                            newNodeId(id),
                            newQualifiedName(id),
                            LocalizedText.english(id));
            getNodeManager().addNode(node);
            return node;
        }

        private UaVariableNode variable(
                UaFolderNode parent, NodeId id, String name, NodeId dataType, DataValue value) {
            UaVariableNode node =
                    UaVariableNode.builder(getNodeContext())
                            .setNodeId(id)
                            .setBrowseName(newQualifiedName(name))
                            .setDisplayName(LocalizedText.english(name))
                            .setDataType(dataType)
                            .setAccessLevel(AccessLevel.CurrentRead)
                            .setUserAccessLevel(AccessLevel.CurrentRead)
                            .setTypeDefinition(Identifiers.BaseDataVariableType)
                            .build();
            node.setValue(value);
            getNodeManager().addNode(node);
            parent.addOrganizes(node);
            return node;
        }

        @Override
        public void onDataItemsCreated(List<DataItem> items) {
            subscriptions.onDataItemsCreated(items);
            monitored.addAndGet(items.size());
        }

        @Override
        public void onDataItemsModified(List<DataItem> items) {
            subscriptions.onDataItemsModified(items);
        }

        @Override
        public void onDataItemsDeleted(List<DataItem> items) {
            subscriptions.onDataItemsDeleted(items);
            monitored.addAndGet(-items.size());
        }

        @Override
        public void onMonitoringModeChanged(List<MonitoredItem> items) {
            subscriptions.onMonitoringModeChanged(items);
        }
    }
}

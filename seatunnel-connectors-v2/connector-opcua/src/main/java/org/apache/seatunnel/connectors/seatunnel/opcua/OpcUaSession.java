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
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.client.UaStackClient;
import org.eclipse.milo.opcua.stack.client.UaStackClientConfig;
import org.eclipse.milo.opcua.stack.client.UaStackClientConfigBuilder;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;

import java.net.ConnectException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaFailure.problem;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ubyte;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

/** Discovers and connects to exactly one configured OPC TCP endpoint. */
final class OpcUaSession implements AutoCloseable {
    private final OpcUaSettings settings;
    private final OpcUaResources resources = new OpcUaResources();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private volatile DiscoveryClient discovery;
    private volatile OpcUaClient client;

    OpcUaSession(OpcUaSettings settings) {
        this.settings = settings;
    }

    OpcUaClient connect(long deadline) throws OpcUaFailure {
        resources.requireHealthy();
        if (stopped.get()) {
            throw problem("session was cancelled");
        }
        if (client != null) {
            return client;
        }
        try {
            EndpointDescription endpoint = endpoint(discover(deadline));
            OpcUaClientConfigBuilder builder =
                    OpcUaClientConfig.builder()
                            .setEndpoint(endpoint)
                            .setApplicationName(LocalizedText.english("Apache SeaTunnel OPC UA"))
                            .setApplicationUri(settings.applicationUri)
                            .setIdentityProvider(new AnonymousProvider())
                            .setSessionTimeout(uint(settings.sessionTimeout))
                            .setMaxPendingPublishRequests(uint(2));
            resources.configure(builder, settings);
            if (!"None".equals(settings.policy)) {
                new OpcUaSecurity(settings).configure(builder, endpoint);
            }
            OpcUaClient opened = OpcUaClient.create(builder.build());
            client = opened;
            if (stopped.get()) {
                opened.disconnect();
                throw problem("session was cancelled");
            }
            await(opened.connect(), deadline, "connect OPC UA session");
            return opened;
        } catch (UaException e) {
            throw problem("could not initialize or configure the OPC UA channel");
        }
    }

    private EndpointDescription[] discover(long deadline) throws OpcUaFailure, UaException {
        String address = settings.endpoint.toASCIIString();
        try {
            return discoverAt(address, deadline);
        } catch (OpcUaFailure failure) {
            if ("None".equals(settings.policy)
                    || address.endsWith("/discovery")
                    || !canUseDiscoveryPath(failure.statusCode)) {
                throw failure;
            }
            // Some servers expose the unsecured GetEndpoints service only on a discovery path.
            String discoveryPath = (address.endsWith("/") ? address : address + "/") + "discovery";
            return discoverAt(discoveryPath, deadline);
        }
    }

    private static boolean canUseDiscoveryPath(long status) {
        return status == StatusCodes.Bad_SecurityChecksFailed
                || status == StatusCodes.Bad_SecurityModeRejected
                || status == StatusCodes.Bad_SecurityPolicyRejected
                || status == StatusCodes.Bad_TcpEndpointUrlInvalid;
    }

    private EndpointDescription[] discoverAt(String address, long deadline)
            throws OpcUaFailure, UaException {
        EndpointDescription bootstrap =
                new EndpointDescription(
                        address,
                        null,
                        null,
                        MessageSecurityMode.None,
                        SecurityPolicy.None.getUri(),
                        null,
                        Stack.TCP_UASC_UABINARY_TRANSPORT_URI,
                        ubyte(0));
        UaStackClientConfigBuilder config = UaStackClientConfig.builder().setEndpoint(bootstrap);
        resources.configure(config, settings);
        DiscoveryClient instance = new DiscoveryClient(UaStackClient.create(config.build()));
        discovery = instance;
        try {
            if (stopped.get()) {
                throw problem("discovery was cancelled");
            }
            await(instance.connect(), deadline, "connect for endpoint discovery");
            return await(
                            instance.getEndpoints(
                                    settings.endpoint.toASCIIString(),
                                    new String[0],
                                    new String[] {Stack.TCP_UASC_UABINARY_TRANSPORT_URI}),
                            deadline,
                            "GetEndpoints")
                    .getEndpoints();
        } finally {
            try {
                await(
                        instance.disconnect(),
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250),
                        "close discovery channel");
            } catch (OpcUaFailure ignored) {
                // The private event loop closes any remaining channels on reader shutdown.
            } finally {
                discovery = null;
            }
        }
    }

    private EndpointDescription endpoint(EndpointDescription[] candidates) throws OpcUaFailure {
        SecurityPolicy policy = SecurityPolicy.valueOf(settings.policy);
        MessageSecurityMode mode = MessageSecurityMode.valueOf(settings.messageMode);
        if (candidates == null) {
            throw problem("GetEndpoints did not return a matching anonymous endpoint");
        }
        for (EndpointDescription candidate : candidates) {
            if (candidate != null
                    && mode == candidate.getSecurityMode()
                    && policy.getUri().equals(candidate.getSecurityPolicyUri())
                    && Stack.TCP_UASC_UABINARY_TRANSPORT_URI.equals(
                            candidate.getTransportProfileUri())
                    && exactAddress(candidate.getEndpointUrl())
                    && candidate.getUserIdentityTokens() != null
                    && Arrays.stream(candidate.getUserIdentityTokens())
                            .filter(Objects::nonNull)
                            .anyMatch(token -> token.getTokenType() == UserTokenType.Anonymous)) {
                return candidate;
            }
        }
        throw problem("GetEndpoints did not return a matching anonymous endpoint");
    }

    private boolean exactAddress(String advertised) {
        try {
            URI uri = URI.create(advertised);
            return "opc.tcp".equals(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getHost().equalsIgnoreCase(settings.endpoint.getHost())
                    && uri.getPort() == settings.endpoint.getPort()
                    && Objects.equals(uri.getRawPath(), settings.endpoint.getRawPath())
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null;
        } catch (IllegalArgumentException | NullPointerException e) {
            return false;
        }
    }

    void validateHealth() throws OpcUaFailure {
        resources.requireHealthy();
    }

    /** Releases the old session before the reader reopens a failed polling channel. */
    void reset() {
        OpcUaClient old = client;
        client = null;
        if (old != null) {
            try {
                await(
                        old.disconnect(),
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(350),
                        "disconnect previous OPC UA session");
            } catch (OpcUaFailure ignored) {
                old.getStackClient().disconnect();
            }
        }
    }

    /** Signals the active channel and closes the private client resources even during discovery. */
    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            try {
                DiscoveryClient discovering = discovery;
                if (discovering != null) {
                    discovering.disconnect();
                }
                OpcUaClient connected = client;
                if (connected != null) {
                    connected.disconnect();
                }
            } finally {
                resources.close();
            }
        }
    }

    static <T> T await(CompletableFuture<T> operation, long deadline, String step)
            throws OpcUaFailure {
        try {
            long allowed = deadline - System.nanoTime();
            if (allowed <= 0L) {
                throw new TimeoutException();
            }
            return operation.get(allowed, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            operation.cancel(true);
            Thread.currentThread().interrupt();
            throw problem(step + " was interrupted");
        } catch (TimeoutException e) {
            operation.cancel(true);
            throw OpcUaFailure.transientProblem(step + " deadline exceeded");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof UaException) {
                long code = ((UaException) cause).getStatusCode().getValue();
                if (code == StatusCodes.Bad_Timeout || code == StatusCodes.Bad_RequestTimeout) {
                    throw OpcUaFailure.transientProblem(
                            step
                                    + " deadline exceeded (OPC UA status 0x"
                                    + Long.toHexString(code)
                                    + ")");
                }
                String text = step + " returned OPC UA status 0x" + Long.toHexString(code);
                return failStatus(code, text);
            }
            if (cause instanceof ClosedChannelException || cause instanceof ConnectException) {
                throw OpcUaFailure.transientProblem(step + " transport failed");
            }
            throw problem(step + " failed");
        } catch (CancellationException e) {
            throw problem(step + " was cancelled");
        }
    }

    private static <T> T failStatus(long code, String text) throws OpcUaFailure {
        boolean retryable =
                code == StatusCodes.Bad_ConnectionClosed
                        || code == StatusCodes.Bad_SecureChannelClosed
                        || code == StatusCodes.Bad_SessionClosed
                        || code == StatusCodes.Bad_SessionIdInvalid
                        || code == StatusCodes.Bad_CommunicationError
                        || code == StatusCodes.Bad_ServerNotConnected;
        throw OpcUaFailure.serviceProblem(text, retryable, code);
    }
}

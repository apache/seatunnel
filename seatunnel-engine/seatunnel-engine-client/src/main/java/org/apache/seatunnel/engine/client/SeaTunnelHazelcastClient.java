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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import lombok.NonNull;

import java.util.UUID;
import java.util.function.Function;

public class SeaTunnelHazelcastClient {
    private final HazelcastClientInstanceImpl hazelcastClient;
    private final SerializationService serializationService;

    public SeaTunnelHazelcastClient(@NonNull ClientConfig clientConfig) {
        Preconditions.checkNotNull(clientConfig, "config");
        this.hazelcastClient =
            ((HazelcastClientProxy) com.hazelcast.client.HazelcastClient.newHazelcastClient(
                clientConfig)).client;
        this.serializationService = hazelcastClient.getSerializationService();
        ExceptionUtil.registerSeaTunnelExceptions(hazelcastClient.getClientExceptionFactory());
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    /**
     * Returns the underlying Hazelcast IMDG instance used by SeaTunnel Engine Client. It will
     * be a client, depending on the type of this
     */
    @NonNull
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastClient;
    }

    public ILogger getLogger(Class<?> clazz) {
        return hazelcastClient.getLoggingService().getLogger(clazz);
    }

    public <S> S requestOnMasterAndDecodeResponse(@NonNull ClientMessage request,
                                                  @NonNull Function<ClientMessage, Object> decoder) {
        UUID masterUuid = hazelcastClient.getClientClusterService().getMasterMember().getUuid();
        return requestAndDecodeResponse(masterUuid, request, decoder);
    }

    public <S> S requestAndDecodeResponse(@NonNull UUID uuid,
                                          @NonNull ClientMessage request,
                                          @NonNull Function<ClientMessage, Object> decoder) {
        ClientInvocation invocation = new ClientInvocation(hazelcastClient, request, null, uuid);
        try {
            ClientMessage response = invocation.invoke().get();
            return serializationService.toObject(decoder.apply(response));
        } catch (InterruptedException i) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public <T> PassiveCompletableFuture<T> requestAndGetCompletableFuture(@NonNull UUID uuid,
                                                                          @NonNull ClientMessage request,
                                                                          @NonNull
                                                                          ClientMessageDecoder<?> clientMessageDecoder) {
        ClientInvocation invocation = new ClientInvocation(hazelcastClient, request, null, uuid);
        try {

            return new PassiveCompletableFuture<>(new ClientDelegatingFuture<>(
                invocation.invoke(),
                serializationService,
                clientMessageDecoder
            ));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public <T> PassiveCompletableFuture<T> requestOnMasterAndGetCompletableFuture(@NonNull ClientMessage request,
                                                                                  @NonNull
                                                                                  ClientMessageDecoder<?> clientMessageDecoder) {
        UUID masterUuid = hazelcastClient.getClientClusterService().getMasterMember().getUuid();
        return requestAndGetCompletableFuture(masterUuid, request, clientMessageDecoder);
    }

    public PassiveCompletableFuture<Void> requestAndGetCompletableFuture(@NonNull UUID uuid,
                                                                         @NonNull ClientMessage request) {
        ClientInvocation invocation = new ClientInvocation(hazelcastClient, request, null, uuid);
        try {
            return new PassiveCompletableFuture<>(invocation.invoke().thenApply(r -> null));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public PassiveCompletableFuture<Void> requestOnMasterAndGetCompletableFuture(@NonNull ClientMessage request) {
        UUID masterUuid = hazelcastClient.getClientClusterService().getMasterMember().getUuid();
        return requestAndGetCompletableFuture(masterUuid, request);
    }

    public void shutdown() {
        if (hazelcastClient != null) {
            hazelcastClient.shutdown();
        }
    }
}

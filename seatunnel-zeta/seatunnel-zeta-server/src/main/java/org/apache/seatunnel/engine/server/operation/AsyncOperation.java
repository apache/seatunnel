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

package org.apache.seatunnel.engine.server.operation;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.stackTraceToString;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;

import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Base class for async operations. Handles registration/deregistration of
 * operations from live registry, exception handling and peeling and
 * logging of exceptions
 */
public abstract class AsyncOperation extends Operation implements IdentifiedDataSerializable {

    @Override
    public void beforeRun() {
        SeaTunnelServer service = getService();
        service.getLiveOperationRegistry().register(this);
    }

    @Override
    public final void run() {
        PassiveCompletableFuture<?> future;
        try {
            future = doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
            return;
        }
        future.whenComplete(withTryCatch(getLogger(), (r, f) -> doSendResponse(f != null ? peel(f) : r)));
    }

    protected abstract PassiveCompletableFuture<?> doRun() throws Exception;

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    private void doSendResponse(Object value) {
        try {
            final SeaTunnelServer service = getService();
            service.getLiveOperationRegistry().deregister(this);
        } finally {
            try {
                sendResponse(value);
            } catch (Exception e) {
                Throwable ex = peel(e);
                if (value instanceof Throwable && ex instanceof HazelcastSerializationException) {
                    // Sometimes exceptions are not serializable, for example on
                    // https://github.com/hazelcast/hazelcast-jet/issues/1995.
                    // When sending exception as a response and the serialization fails,
                    // the response will not be sent and the operation will hang.
                    // To prevent this from happening, replace the exception with
                    // another exception that can be serialized.
                    sendResponse(new SeaTunnelEngineException(stackTraceToString(ex)));
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isRestartableException(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

    @Override
    public final int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }
}

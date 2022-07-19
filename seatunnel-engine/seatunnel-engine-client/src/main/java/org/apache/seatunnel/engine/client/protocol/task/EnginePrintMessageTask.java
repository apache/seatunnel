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

package org.apache.seatunnel.engine.client.protocol.task;

import org.apache.seatunnel.engine.client.protocol.codec.EnginePrintMessageCodec;
import org.apache.seatunnel.engine.operation.PrintMessageOperation;

import com.google.common.base.Function;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class EnginePrintMessageTask extends AbstractInvocationMessageTask<String> {

    protected EnginePrintMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (masterAddress == null) {
            throw new RetryableHazelcastException("master not yet known");
        }
        return nodeEngine.getOperationService().createInvocationBuilder(JetService.SERVICE_NAME,
            op, masterAddress);
    }

    @Override
    protected Operation prepareOperation() {
        return new PrintMessageOperation(parameters);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        Function<ClientMessage, String> decodeRequest = EnginePrintMessageCodec::decodeRequest;
        return decodeRequest.apply(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Function<String, ClientMessage> encodeResponse = EnginePrintMessageCodec::encodeResponse;
        return encodeResponse.apply((String) response);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "printMessage";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}

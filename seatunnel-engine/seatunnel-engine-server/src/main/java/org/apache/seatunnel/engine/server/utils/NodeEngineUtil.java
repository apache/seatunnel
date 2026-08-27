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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

public class NodeEngineUtil {

    private NodeEngineUtil() {}

    public static <E> InvocationFuture<E> sendOperationToMasterNode(
            NodeEngine nodeEngine, Operation operation) {
        InvocationBuilder invocationBuilder =
                nodeEngine
                        .getOperationService()
                        .createInvocationBuilder(
                                SeaTunnelServer.SERVICE_NAME,
                                operation,
                                nodeEngine.getMasterAddress())
                        .setAsync();
        return invocationBuilder.invoke();
    }

    public static <E> InvocationFuture<E> sendOperationToMemberNode(
            NodeEngine nodeEngine, Operation operation, Address memberAddress) {
        InvocationBuilder invocationBuilder =
                nodeEngine
                        .getOperationService()
                        .createInvocationBuilder(
                                SeaTunnelServer.SERVICE_NAME, operation, memberAddress)
                        .setAsync();
        return invocationBuilder.invoke();
    }
}

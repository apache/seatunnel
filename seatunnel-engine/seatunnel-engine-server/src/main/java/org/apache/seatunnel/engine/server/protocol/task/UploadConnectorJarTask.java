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

package org.apache.seatunnel.engine.server.protocol.task;

import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelUploadConnectorJarCodec;
import org.apache.seatunnel.engine.server.operation.UploadConnectorJarOperation;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

public class UploadConnectorJarTask
        extends AbstractSeaTunnelMessageTask<
                SeaTunnelUploadConnectorJarCodec.RequestParameters, Data> {

    protected UploadConnectorJarTask(
            ClientMessage clientMessage, Node node, Connection connection) {
        super(
                clientMessage,
                node,
                connection,
                SeaTunnelUploadConnectorJarCodec::decodeRequest,
                SeaTunnelUploadConnectorJarCodec::encodeResponse);
    }

    @Override
    protected Operation prepareOperation() {
        return new UploadConnectorJarOperation(parameters.jobId, parameters.connectorJar);
    }

    @Override
    public String getMethodName() {
        return "uploadConnectorJar";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}

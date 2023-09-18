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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.ServerConnectorPackageClient;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

public class DeleteConnectorJarInExecutionNode extends Operation
        implements IdentifiedDataSerializable {
    private ConnectorJarIdentifier connectorJarIdentifier;

    public DeleteConnectorJarInExecutionNode() {}

    public DeleteConnectorJarInExecutionNode(ConnectorJarIdentifier connectorJarIdentifier) {
        this.connectorJarIdentifier = connectorJarIdentifier;
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.DELETE_CONNECTOR_JAR_IN_EXECUTION_NODE;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer seaTunnelServer = getService();
        ServerConnectorPackageClient serverConnectorPackageClient =
                seaTunnelServer.getTaskExecutionService().getServerConnectorPackageClient();
        serverConnectorPackageClient.deleteConnectorJar(connectorJarIdentifier);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(connectorJarIdentifier);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.connectorJarIdentifier = in.readObject();
    }
}

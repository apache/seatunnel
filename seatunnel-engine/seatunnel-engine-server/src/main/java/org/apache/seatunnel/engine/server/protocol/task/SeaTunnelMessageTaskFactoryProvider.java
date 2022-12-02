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

import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelCancelJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobStateCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelListJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelPrintMessageCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelSubmitJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelWaitForJobCompleteCodec;

import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class SeaTunnelMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private final Int2ObjectHashMap<MessageTaskFactory> factories = new Int2ObjectHashMap<>(60);
    public final Node node;

    public SeaTunnelMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return this.factories;
    }

    private void initFactories() {
        factories.put(SeaTunnelPrintMessageCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new PrintMessageTask(clientMessage, node, connection));
        factories.put(SeaTunnelSubmitJobCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new SubmitJobTask(clientMessage, node, connection));
        factories.put(SeaTunnelWaitForJobCompleteCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new WaitForJobCompleteTask(clientMessage, node, connection));
        factories.put(SeaTunnelCancelJobCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new CancelJobTask(clientMessage, node, connection));
        factories.put(SeaTunnelGetJobStatusCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new GetJobStatusTask(clientMessage, node, connection));
        factories.put(SeaTunnelGetJobStateCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new GetJobStateTask(clientMessage, node, connection));
        factories.put(SeaTunnelListJobStatusCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new ListJobStatusTask(clientMessage, node, connection));
        factories.put(SeaTunnelGetJobMetricsCodec.REQUEST_MESSAGE_TYPE,
            (clientMessage, connection) -> new GetJobMetricsTask(clientMessage, node, connection));
    }
}

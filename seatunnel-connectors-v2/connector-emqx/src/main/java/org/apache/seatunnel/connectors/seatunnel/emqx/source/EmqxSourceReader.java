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

package org.apache.seatunnel.connectors.seatunnel.emqx.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.emqx.client.MqttClientUtil;
import org.apache.seatunnel.connectors.seatunnel.emqx.config.MessageFormatErrorHandleWay;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class EmqxSourceReader implements SourceReader<SeaTunnelRow, EmqxSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;
    private final SourceReader.Context context;
    private MqttClient client;
    private final ClientMetadata metadata;
    private EmqxSourceSplit split;
    private final Queue<byte[]> payloadQueue;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final MessageFormatErrorHandleWay messageFormatErrorHandleWay;

    EmqxSourceReader(
            ClientMetadata metadata,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            Context context,
            MessageFormatErrorHandleWay messageFormatErrorHandleWay) {
        this.metadata = metadata;
        this.context = context;
        this.messageFormatErrorHandleWay = messageFormatErrorHandleWay;
        this.deserializationSchema = deserializationSchema;
        this.payloadQueue = new LinkedBlockingQueue<>(1024);
    }

    @Override
    public void open() throws MqttException {
        client = MqttClientUtil.createMqttClient(metadata, context.getIndexOfSubtask());
        client.setCallback(
                new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        log.warn(
                                "connection lost and reconnect , error message: {}",
                                cause.getMessage());
                        if (client != null && !client.isConnected()) {
                            try {
                                client.reconnect();
                            } catch (MqttException e) {
                                log.error("mqtt client reconnect error", e);
                                throw new SeaTunnelException("mqtt client reconnect error", e);
                            }
                        }
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        payloadQueue.offer(message.getPayload());
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        if (log.isDebugEnabled()) {
                            log.debug("deliveryComplete {}", token.isComplete());
                        }
                    }
                });
        client.subscribe(metadata.getTopic(), metadata.getQos());
    }

    @Override
    public void close() throws IOException {
        try {
            if (client.isConnected()) {
                client.disconnect();
            }
            client.close();
        } catch (MqttException e) {
            log.error("mqtt client close error", e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (split == null) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // make sure client not receive new message
            client.disconnect();
        }
        while (!payloadQueue.isEmpty()) {
            byte[] payload = payloadQueue.poll();
            try {
                deserializationSchema.deserialize(payload, output);
            } catch (Exception e) {
                String msg = new String(payload, StandardCharsets.UTF_8);
                if (messageFormatErrorHandleWay.equals(MessageFormatErrorHandleWay.SKIP)) {
                    log.error("deserialize message \"{}\" error, skip this message", msg, e);
                } else {
                    throw new SeaTunnelException("deserialize message \"" + msg + "\" error", e);
                }
            }
        }
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<EmqxSourceSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void addSplits(List<EmqxSourceSplit> splits) {
        split = splits.get(0);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this reader will not add new split.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}

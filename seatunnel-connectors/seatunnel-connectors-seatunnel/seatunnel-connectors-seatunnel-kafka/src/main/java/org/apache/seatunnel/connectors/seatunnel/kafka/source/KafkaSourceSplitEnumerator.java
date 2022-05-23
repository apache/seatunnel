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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaState;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KafkaSourceSplitEnumerator implements SourceSplitEnumerator<KafkaSourceSplit, KafkaState> {

    private static final String CLIENT_ID_PREFIX = "seatunnel";

    private final String topic;
    private final String bootstrapServer;
    private KafkaConsumer<byte[], byte[]> consumer;
    private AdminClient adminClient;

    KafkaSourceSplitEnumerator(String topic, String bootstrapServer, Properties properties) {
        this.topic = topic;
        this.bootstrapServer = bootstrapServer;
    }

    @Override
    public void open() {
        this.consumer = initConsumer();
        this.adminClient = initAdminClient();
    }

    @Override
    public void run() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<KafkaSourceSplit> splits, int subtaskId) {

    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {

    }

    @Override
    public KafkaState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private KafkaConsumer<byte[], byte[]> initConsumer() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + "-enumerator-consumer");

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        // Disable auto create topics feature
        props.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    private AdminClient initAdminClient() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + "-enumerator-admin-client");
        return AdminClient.create(props);
    }

}

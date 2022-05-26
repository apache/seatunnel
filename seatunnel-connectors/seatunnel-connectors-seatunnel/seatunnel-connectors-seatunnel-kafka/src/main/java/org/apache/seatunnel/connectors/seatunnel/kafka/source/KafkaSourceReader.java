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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;

import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaSourceReader implements SourceReader<SeaTunnelRow, KafkaSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;
    private static final long POLL_TIMEOUT = 10000L;
    private static final String CLIENT_ID_PREFIX = "seatunnel";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceReader.class);

    private final SourceReader.Context context;
    private KafkaConsumer<byte[], byte[]> consumer;
    private final ConsumerMetadata metadata;
    private final Set<KafkaSourceSplit> sourceSplits;
    private final Map<TopicPartition, Long> endOffset;
    // TODO support user custom type
    private SeaTunnelRowTypeInfo typeInfo;

    KafkaSourceReader(ConsumerMetadata metadata, SeaTunnelRowTypeInfo typeInfo,
                      SourceReader.Context context) {
        this.metadata = metadata;
        this.context = context;
        this.typeInfo = typeInfo;
        this.sourceSplits = new HashSet<>();
        this.endOffset = new HashMap<>();
    }

    @Override
    public void open() {
        this.consumer = initConsumer(this.metadata.getBootstrapServer(), this.metadata.getConsumerGroup(),
                this.metadata.getProperties());
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (sourceSplits.isEmpty() || sourceSplits.size() != this.endOffset.size()) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        Set<TopicPartition> partitions = convertToPartition(sourceSplits);
        StringDeserializer stringDeserializer = new StringDeserializer();
        stringDeserializer.configure(Maps.fromProperties(this.metadata.getProperties()), false);
        consumer.assign(partitions);
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
            for (TopicPartition partition : partitions) {
                for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {

                    String v = stringDeserializer.deserialize(partition.topic(), record.value());
                    String t = partition.topic();
                    output.collect(new SeaTunnelRow(new Object[]{t, v}));

                    if (Boundedness.BOUNDED.equals(context.getBoundedness()) &&
                            record.offset() >= endOffset.get(partition)) {
                        break;
                    }
                }
            }

            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                context.signalNoMoreElement();
                break;
            }
        }
    }

    @Override
    public List<KafkaSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<KafkaSourceSplit> splits) {
        sourceSplits.addAll(splits);
        sourceSplits.forEach(partition -> {
            endOffset.put(partition.getTopicPartition(), partition.getEndOffset());
        });
    }

    @Override
    public void handleNoMoreSplits() {
        LOGGER.info("receive no more splits message, this reader will not add new split.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // TODO commit offset
    }

    private KafkaConsumer<byte[], byte[]> initConsumer(String bootstrapServer, String consumerGroup,
                                                       Properties properties) {
        Properties props = new Properties(properties);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + "-enumerator-consumer");

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        // Disable auto create topics feature
        props.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    private Set<TopicPartition> convertToPartition(Collection<KafkaSourceSplit> sourceSplits) {
        return sourceSplits.stream().map(KafkaSourceSplit::getTopicPartition).collect(Collectors.toSet());
    }

}

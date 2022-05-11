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

package org.apache.seatunnel.e2e.flink;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

public abstract class FlinkKafkaContainer extends FlinkContainer{
    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainer.class);
    protected KafkaContainer kafka;

    @Before
    public void initKafka() {
        this.kafka = new KafkaContainer()
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka")
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .withEmbeddedZookeeper();
        kafka.setPortBindings(Lists.newArrayList("9092:9092", "9093:9093"));
        kafka.setHostAccessible(true);

        Startables.deepStart(Stream.of(kafka)).join();
        LOG.info("Kafka container start.");
    }

    @After
    public void close() {
        if (kafka != null) {
            kafka.stop();
        }
    }
}

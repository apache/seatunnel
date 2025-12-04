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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Multi-table resource manager for Kafka sink.
 *
 * <p>Manages the shared KafkaProducerManager across multiple table writers.
 */
@AllArgsConstructor
@Slf4j
public class KafkaMultiTableResourceManager
        implements MultiTableResourceManager<KafkaProducerManager> {

    private final KafkaProducerManager producerManager;

    @Override
    public Optional<KafkaProducerManager> getSharedResource() {
        return Optional.of(producerManager);
    }

    @Override
    public void close() {
        log.info("Closing Kafka multi-table resource manager");
        if (producerManager != null) {
            producerManager.close();
        }
    }
}

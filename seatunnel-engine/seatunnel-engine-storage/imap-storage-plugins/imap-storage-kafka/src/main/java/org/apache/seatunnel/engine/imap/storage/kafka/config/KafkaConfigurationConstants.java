/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.kafka.config;

public class KafkaConfigurationConstants {
    public static final String BUSINESS_KEY = "businessName";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KAFKA_STORAGE_COMPACT_TOPIC_PREFIX = "storage.compact.topic.prefix";
    public static final String KAFKA_STORAGE_COMPACT_TOPIC_REPLICATION_FACTOR =
            "storage.compact.topic.replication.factor";
    public static final String KAFKA_STORAGE_COMPACT_TOPIC_PARTITION =
            "storage.compact.topic.partition";

    public static final String KAFKA_CONSUMER_CONFIGS_PREFIX = "consumer.override.";
    public static final String KAFKA_PRODUCER_CONFIGS_PREFIX = "producer.override.";
    public static final String KAFKA_ADMIN_CONFIGS_PREFIX = "admin.override.";
    public static final String KAFKA_TOPIC_CONFIGS_PREFIX = "topic.override.";
}

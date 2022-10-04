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

package org.apache.seatunnel.connectors.seatunnel.kafka.config;

public class Config {
    /**
     * The topic of kafka.
     */
    public static final String TOPIC = "topic";

    /**
     * The topic of kafka is java pattern or list.
     */
    public static final String PATTERN = "pattern";

    /**
     * The server address of kafka cluster.
     */
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String KAFKA_CONFIG_PREFIX = "kafka.";

    /**
     * consumer group of kafka client consume message.
     */
    public static final String CONSUMER_GROUP = "consumer.group";

    /**
     * consumer's offset will be periodically committed in the background.
     */
    public static final String COMMIT_ON_CHECKPOINT = "commit_on_checkpoint";

    /**
     * The prefix of kafka's transactionId, make sure different job use different prefix.
     */
    public static final String TRANSACTION_PREFIX = "transaction_prefix";

    /**
     * Send information according to the specified partition.
     */
    public static final String PARTITION = "partition";
}

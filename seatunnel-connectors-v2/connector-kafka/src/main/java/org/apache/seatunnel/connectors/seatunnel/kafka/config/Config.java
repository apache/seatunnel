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

    public static final String PRODUCER_CONFIG_PREFIX = "producer.override.";

    public static final String CONSUMER_CONFIG_PREFIX = "consumer.override.";

    /**
     * consumer group of kafka client consume message.
     */
    public static final String CONSUMER_GROUP = "consumer.group";

    /**
     * consumer offset will be periodically committed in the background.
     */
    public static final String COMMIT_ON_CHECKPOINT = "commit_on_checkpoint";

    /**
     * The prefix of kafka's transactionId, make sure different job use different prefix.
     */
    public static final String TRANSACTION_PREFIX = "transaction_prefix";

    /**
     * User-defined schema
     */
    public static final String SCHEMA = "schema";

    /**
     * data format
     */
    public static final String FORMAT = "format";

    /**
     * The default data format is JSON
     */
    public static final String DEFAULT_FORMAT = "json";

    /**
     * field delimiter
     */
    public static final String FIELD_DELIMITER = "field_delimiter";

    /**
     * The default field delimiter is “,”
     */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    /**
     * Send information according to the specified partition.
     */
    public static final String PARTITION = "partition";

    /**
     * Determine the partition to send based on the content of the message.
     */
    public static final String ASSIGN_PARTITIONS = "assign_partitions";

    /**
     * Determine the key of the kafka send partition
     */
    public static final String PARTITION_KEY = "partition_key";

}

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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class Config {

    /**
     * The default data format is JSON
     */
    public static final String DEFAULT_FORMAT = "json";

    /**
     * The default field delimiter is “,”
     */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    /**
     * Kafka config prefix
     */
    public static final String KAFKA_CONFIG_PREFIX = "kafka.";

    public static final Option<String> TOPIC = Options.key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("The topic of kafka");

    public static final Option<Boolean> PATTERN = Options.key("pattern")
            .booleanType()
            .defaultValue(false)
            .withDescription("The topic of kafka is java pattern or list");

    public static final Option<String> BOOTSTRAP_SERVERS = Options.key("bootstrap.servers")
            .stringType()
            .noDefaultValue()
            .withDescription("The server address of kafka cluster");

    public static final Option<String> CONSUMER_GROUP = Options.key("consumer.group")
            .stringType()
            .noDefaultValue()
            .withDescription("consumer group of kafka client consume message");

    public static final Option<Boolean> COMMIT_ON_CHECKPOINT = Options.key("commit_on_checkpoint")
            .booleanType()
            .defaultValue(true)
            .withDescription("consumer offset will be periodically committed in the background");

    public static final Option<String> TRANSACTION_PREFIX = Options.key("transaction_prefix")
            .stringType()
            .noDefaultValue()
            .withDescription("The prefix of kafka's transactionId, make sure different job use different prefix");

    public static final Option<Config> SCHEMA = Options.key("schema")
            .objectType(Config.class)
            .noDefaultValue()
            .withDescription("user-defined schema");

    public static final Option<String> FORMAT = Options.key("format")
            .stringType()
            .noDefaultValue()
            .withDescription("data format");

    public static final Option<String> FIELD_DELIMITER = Options.key("field_delimiter")
            .stringType()
            .noDefaultValue()
            .withDescription("field delimiter");

    public static final Option<Integer> PARTITION = Options.key("partition")
            .intType()
            .noDefaultValue()
            .withDescription("send information according to the specified partition");

    public static final Option<List<String>> ASSIGN_PARTITIONS = Options.key("assign_partitions")
            .listType()
            .noDefaultValue()
            .withDescription("determine the partition to send based on the content of the message");

    public static final Option<String> PARTITION_KEY = Options.key("partition_key")
            .stringType()
            .noDefaultValue()
            .withDescription("determine the key of the kafka send partition");

    public static final Option<String> START_MODE = Options.key("start_mode")
            .stringType()
            .defaultValue("group_offsets")
            .withDescription("The initial consumption pattern of consumers");

    public static final Option<Long> START_MODE_TIMESTAMP = Options.key("start_mode.timestamp")
            .longType()
            .noDefaultValue()
            .withDescription("The time required for consumption mode to be timestamp");

    public static final Option<Config> START_MODE_OFFSETS = Options.key("start_mode.offsets")
            .objectType(Config.class)
            .noDefaultValue()
            .withDescription("The offset required for consumption mode to be specific_offsets");

}

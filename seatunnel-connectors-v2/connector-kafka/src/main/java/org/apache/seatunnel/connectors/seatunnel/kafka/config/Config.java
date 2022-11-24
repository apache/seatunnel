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

    public static final Option<String> KAFKA_CONFIG_PREFIX = Options.key("kafka.")
            .stringType()
            .noDefaultValue()
            .withDescription("In addition to the above parameters that must be specified by the Kafka producer or consumer client, " +
                    "the user can also specify multiple non-mandatory parameters for the producer or consumer client, " +
                    "covering all the producer parameters specified in the official Kafka document.");

    public static final Option<String> TOPIC = Options.key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("Kafka topic name. If there are multiple topics, use , to split, for example: \"tpc1,tpc2\".");

    public static final Option<Boolean> PATTERN = Options.key("pattern")
            .booleanType()
            .defaultValue(false)
            .withDescription("If pattern is set to true,the regular expression for a pattern of topic names to read from." +
                    " All topics in clients with names that match the specified regular expression will be subscribed by the consumer.");

    public static final Option<String> BOOTSTRAP_SERVERS = Options.key("bootstrap.servers")
            .stringType()
            .noDefaultValue()
            .withDescription("Kafka cluster address, separated by \",\".");

    public static final Option<String> CONSUMER_GROUP = Options.key("consumer.group")
            .stringType()
            .noDefaultValue()
            .withDescription("Kafka consumer group id, used to distinguish different consumer groups.");

    public static final Option<Boolean> COMMIT_ON_CHECKPOINT = Options.key("commit_on_checkpoint")
            .booleanType()
            .defaultValue(true)
            .withDescription("If true the consumer's offset will be periodically committed in the background.");

    public static final Option<String> TRANSACTION_PREFIX = Options.key("transaction_prefix")
            .stringType()
            .noDefaultValue()
            .withDescription("If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a Kafka transaction. " +
                    "Kafka distinguishes different transactions by different transactionId. " +
                    "This parameter is prefix of kafka transactionId, make sure different job use different prefix.");

    public static final Option<Config> SCHEMA = Options.key("schema")
            .objectType(Config.class)
            .noDefaultValue()
            .withDescription("The structure of the data, including field names and field types.");

    public static final Option<String> FORMAT = Options.key("format")
            .stringType()
            .noDefaultValue()
            .withDescription("Data format. The default format is json. Optional text format. The default field separator is \", \". " +
                    "If you customize the delimiter, add the \"field_delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER = Options.key("field_delimiter")
            .stringType()
            .noDefaultValue()
            .withDescription("Customize the field delimiter for data format.");

    public static final Option<Integer> PARTITION = Options.key("partition")
            .intType()
            .noDefaultValue()
            .withDescription("We can specify the partition, all messages will be sent to this partition.");

    public static final Option<List<String>> ASSIGN_PARTITIONS = Options.key("assign_partitions")
            .listType()
            .noDefaultValue()
            .withDescription("We can decide which partition to send based on the content of the message. " +
                    "The function of this parameter is to distribute information.");

    public static final Option<List<String>> PARTITION_KEY_FIELDS = Options.key("partition_key_fields")
            .listType()
            .noDefaultValue()
            .withDescription("Configure which fields are used as the key of the kafka message.");

    public static final Option<StartMode> START_MODE = Options.key("start_mode")
            .objectType(StartMode.class)
            .defaultValue(StartMode.GROUP_OFFSETS)
            .withDescription("The initial consumption pattern of consumers,there are several types:\n" +
                    "[earliest],[group_offsets],[latest],[specific_offsets],[timestamp]");

    public static final Option<Long> START_MODE_TIMESTAMP = Options.key("start_mode.timestamp")
            .longType()
            .noDefaultValue()
            .withDescription("The time required for consumption mode to be timestamp.");

    public static final Option<Config> START_MODE_OFFSETS = Options.key("start_mode.offsets")
            .objectType(Config.class)
            .noDefaultValue()
            .withDescription("The offset required for consumption mode to be specific_offsets.");

    /**
     * Configuration key to define the consumer's partition discovery interval, in milliseconds.
     */
    public static final Option<Long> KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = Options.key("partition-discovery.interval-millis")
        .longType()
        .defaultValue(-1L)
        .withDescription("The interval for dynamically discovering topics and partitions.");

}

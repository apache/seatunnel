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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.apache.pulsar.client.api.MessageRoutingMode;

import java.util.List;
import java.util.Map;

public class SinkProperties {

    /** The default data format is JSON */
    public static final String DEFAULT_FORMAT = "json";

    public static final String TEXT_FORMAT = "text";

    /** The default field delimiter is “,” */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue(DEFAULT_FORMAT)
                    .withDescription(
                            "Data format. The default format is json. Optional text format. The default field separator is \", \". "
                                    + "If you customize the delimiter, add the \"field_delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription(
                            "Customize the field delimiter for data format.The default field_delimiter is ',' ");
    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sink pulsar topic name.");
    public static final Option<PulsarSemantics> SEMANTICS =
            Options.key("semantics")
                    .enumType(PulsarSemantics.class)
                    .defaultValue(PulsarSemantics.AT_LEAST_ONCE)
                    .withDescription(
                            "If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a Pulsar transaction.");

    public static final Option<Integer> TRANSACTION_TIMEOUT =
            Options.key("transaction_timeout")
                    .intType()
                    .defaultValue(600)
                    .withDescription(
                            "The transaction timeout is specified as 10 minutes by default. If the transaction does not commit within the specified timeout, the transaction will be automatically aborted. So you need to ensure that the timeout is greater than the checkpoint interval");

    public static final Option<Map<String, String>> PULSAR_CONFIG =
            Options.key("pulsar.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "In addition to the above parameters that must be specified by the Pulsar producer or consumer client, "
                                    + "the user can also specify multiple non-mandatory parameters for the producer or consumer client, "
                                    + "covering all the producer parameters specified in the official Pulsar document.");

    public static final Option<MessageRoutingMode> MESSAGE_ROUTING_MODE =
            Options.key("message.routing.mode")
                    .enumType(MessageRoutingMode.class)
                    .defaultValue(MessageRoutingMode.RoundRobinPartition)
                    .withDescription(
                            "Default routing mode for messages to partition. "
                                    + "If you choose SinglePartition，If no key is provided, The partitioned producer will randomly pick one single partition and publish all the messages into that partition. "
                                    + " If a key is provided on the message, the partitioned producer will hash the key and assign message to a particular partition."
                                    + " If you choose RoundRobinPartition，If no key is provided, the producer will publish messages across all partitions in round-robin fashion to achieve maximum throughput. "
                                    + "Please note that round-robin is not done per individual message but rather it's set to the same boundary of batching delay, to ensure batching is effective.");

    public static final Option<List<String>> PARTITION_KEY_FIELDS =
            Options.key("partition_key_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Configure which fields are used as the key of the pulsar message.");
}
